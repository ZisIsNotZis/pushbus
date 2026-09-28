"""A small, dependency-free push bus.

The module retains the original ``subpub``/``pub`` convenience API and also
exposes :class:`PushBus` for applications that need an independent bus.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from inspect import Parameter, Signature, signature
import itertools
import logging
import re
from threading import RLock, Timer
import time
import types
from typing import Any, Optional, Union, get_args, get_origin, get_type_hints


logger = logging.getLogger(__name__)

# A callback can use this for provenance without changing the deliberately
# plain value-based callback API.
_provenance: ContextVar[Any] = ContextVar("push_bus_provenance", default=None)


def current_provenance() -> Any:
    """Return provenance for the currently executing user callback."""
    return _provenance.get()


def safe_repr(value: Any, limit: int = 100) -> str:
    """Return a bounded repr, including for objects with a broken __repr__."""
    try:
        text = repr(value)
    except Exception:
        text = "<unrepresentable>"
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


@dataclass(frozen=True)
class _Item:
    value: Any
    path: str
    captures: dict[str, str]
    provenance: Any
    sequence: int


@dataclass
class _KeyState:
    buffers: dict[str, list[_Item]]
    next_sequence: int = 0
    last_emitted_sequence: int = 0
    pending: bool = False
    quiet_generation: int = 0
    max_generation: int = 0
    max_deadline: float | None = None
    quiet_timer: Timer | None = None
    max_timer: Timer | None = None


@dataclass
class _NodeStats:
    calls: int = 0
    successes: int = 0
    failures: int = 0
    last_error: str | None = None
    total_duration: float = 0.0
    last_duration: float | None = None
    last_call_time: float | None = None


@dataclass
class _EdgeStats:
    calls: int = 0
    last_call_time: float | None = None


@dataclass
class _Port:
    name: str
    parameter: str
    pattern: re.Pattern[str]
    annotation: Any
    optional: bool


@dataclass
class _Node:
    name: str
    function: Callable[..., Any]
    ports: list[_Port]
    outputs: list[str]
    shared_groups: tuple[str, ...]
    grace_period: float = 0.0
    max_grace_period: float | None = None
    consume: bool = True
    states: dict[tuple[Any, ...], _KeyState] = field(default_factory=dict)
    lock: RLock = field(default_factory=RLock)
    stats: _NodeStats = field(default_factory=_NodeStats)
    edge_stats: dict[tuple[str, tuple[Any, ...]], _EdgeStats] = field(default_factory=dict)
    active: bool = True


def _is_optional(annotation: Any) -> bool:
    if annotation is Parameter.empty or annotation is Any:
        return False
    origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        return type(None) in get_args(annotation)
    return annotation is Optional[Any]  # kept for unusual typing implementations


def _accepts(annotation: Any, value: Any) -> bool:
    if annotation is Parameter.empty or annotation is Any or annotation is object:
        return True
    if _is_optional(annotation):
        return value is None or any(
            arg is not type(None) and _accepts(arg, value) for arg in get_args(annotation)
        )
    try:
        return isinstance(value, annotation)
    except TypeError:
        # An unresolved forward reference or unsupported typing form should not
        # make an otherwise valid route unusable.
        return True


def _compile(pattern: str | re.Pattern[str]) -> re.Pattern[str]:
    return pattern if hasattr(pattern, "fullmatch") else re.compile(pattern)


def _normalise_outputs(outputs: str | Sequence[str] | None) -> list[str]:
    if outputs is None:
        return [""]
    if isinstance(outputs, str):
        return [outputs]
    return list(outputs)


class Disposable:
    """Idempotent handle for removing a registered subscription."""

    def __init__(self, dispose: Callable[[], None]) -> None:
        self._dispose = dispose
        self.disposed = False

    def dispose(self) -> None:
        if not self.disposed:
            self.disposed = True
            self._dispose()

    close = dispose


class PushBus:
    """Thread-safe push dispatcher with named, multi-input subscriptions."""

    def __init__(self) -> None:
        self._nodes: list[_Node] = []
        self._lock = RLock()
        self._node_number = 0

    def clear(self) -> None:
        """Remove subscriptions and cancel outstanding timers."""
        with self._lock:
            nodes, self._nodes = self._nodes, []
        for node in nodes:
            with node.lock:
                for state in node.states.values():
                    if state.quiet_timer:
                        state.quiet_timer.cancel()
                    if state.max_timer:
                        state.max_timer.cancel()
                node.states.clear()

    def subscribe(
        self,
        *input_paths: str | re.Pattern[str],
        _inputs: str | Mapping[str, str | re.Pattern[str]] | Sequence[str | re.Pattern[str]] | None = None,
        _outputs: str | Sequence[str] | None = "",
        _grace_period: float = 0.0,
        _max_grace_period: float | None = None,
        _consume: bool = True,
        _name: str | None = None,
        **_named_inputs: str | re.Pattern[str] | Sequence[str],
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorate and register a callback.

        Positional paths are split at the callback's input-parameter count:
        remaining paths are output templates. Underscored keywords are
        reserved options (for example ``_outputs`` and ``_consume``).

        ``inputs`` may be one path, a sequence in callback parameter order, or
        a mapping of named input ports to paths. Mapping keys are port names;
        callback parameters remain positional and ordinary Python parameters.
        """
        inputs = _inputs
        outputs = _outputs
        grace_period = _grace_period
        max_grace_period = _max_grace_period
        consume = _consume
        name = _name
        if grace_period < 0 or (max_grace_period is not None and max_grace_period < 0):
            raise ValueError("grace periods must be non-negative")

        if _named_inputs:
            if input_paths or inputs is not None:
                raise TypeError("use named, positional, or _inputs= paths, not multiple forms")
            inputs = _named_inputs
        if input_paths and inputs is not None:
            raise TypeError("use positional input paths or _inputs=, not both")
        if input_paths:
            inputs = input_paths
        if inputs is None:
            inputs = ""

        def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
            positional_outputs: Sequence[str] = ()
            actual_inputs = inputs
            if input_paths:
                parameter_count = len([
                    p for p in signature(function).parameters.values()
                    if p.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY)
                ])
                if len(input_paths) > parameter_count:
                    actual_inputs = input_paths[:parameter_count]
                    positional_outputs = input_paths[parameter_count:]
            actual_outputs = outputs
            if positional_outputs:
                if outputs not in (None, ""):
                    raise TypeError("outputs supplied both positionally and by keyword")
                actual_outputs = positional_outputs
            node = self._make_node(function, actual_inputs, actual_outputs, name)
            # Timers are configured on the node so registration remains cheap.
            node.grace_period = float(grace_period)
            node.max_grace_period = None if max_grace_period is None else float(max_grace_period)
            node.consume = bool(consume)
            with self._lock:
                self._nodes.append(node)
            handle = Disposable(lambda: self._remove(node))
            # Keep decorator ergonomics while exposing explicit disposal.
            setattr(function, "subscription", handle)
            setattr(function, "dispose", handle.dispose)
            return function

        return decorator

    def _remove(self, node: _Node) -> None:
        with self._lock:
            node.active = False
            if node in self._nodes:
                self._nodes.remove(node)
        with node.lock:
            for state in node.states.values():
                if state.quiet_timer:
                    state.quiet_timer.cancel()
                if state.max_timer:
                    state.max_timer.cancel()
            node.states.clear()

    def _make_node(
        self,
        function: Callable[..., Any],
        inputs: str | Mapping[str, str | re.Pattern[str]] | Sequence[str | re.Pattern[str]],
        outputs: str | Sequence[str] | None,
        name: str | None,
    ) -> _Node:
        sig: Signature = signature(function)
        parameters = [
            p for p in sig.parameters.values()
            if p.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY)
        ]
        try:
            annotations = get_type_hints(function)
        except Exception:
            annotations = {}

        if isinstance(inputs, Mapping):
            input_items = list(inputs.items())
        elif isinstance(inputs, str) or hasattr(inputs, "fullmatch"):
            input_items = [(parameters[0].name if parameters else "value", inputs)]
        else:
            input_items = [
                (parameters[index].name if index < len(parameters) else f"input{index}", path)
                for index, path in enumerate(inputs)
            ]
        if not input_items:
            raise ValueError("a subscription needs at least one input")
        if len(parameters) < len(input_items):
            raise ValueError("there are more input ports than callback parameters")

        ports: list[_Port] = []
        for index, (port_name, path) in enumerate(input_items):
            parameter = parameters[index]
            annotation = annotations.get(parameter.name, parameter.annotation)
            ports.append(
                _Port(
                    str(port_name),
                    parameter.name,
                    _compile(path),
                    annotation,
                    _is_optional(annotation),
                )
            )
        group_sets = [set(port.pattern.groupindex) for port in ports]
        shared = tuple(
            group for group in ports[0].pattern.groupindex
            if all(group in groups for groups in group_sets[1:])
        )
        with self._lock:
            self._node_number += 1
            node_name = name or getattr(function, "__name__", "subscription")
            node_name = f"{node_name}#{self._node_number}"
        node = _Node(node_name, function, ports, _normalise_outputs(outputs), shared)
        return node

    def publish(self, value: Any, path: str = "", *, _parents: Any = None) -> None:
        """Push one value through all matching subscriptions in registration order."""
        logger.info("push path=%s payload=%s parents=%s", path, safe_repr(value), safe_repr(_parents))
        # Snapshot registration is intentional: callbacks may register routes,
        # but a route registered during this publish starts on the next publish.
        with self._lock:
            nodes = tuple(self._nodes)
        for node in nodes:
            if node.active:
                self._deliver(node, value, path, _parents)

    pub = publish

    def _deliver(self, node: _Node, value: Any, path: str, parents: Any) -> None:
        ready_states: list[tuple[tuple[Any, ...], _KeyState, int | None]] = []
        with node.lock:
            for port in node.ports:
                if not _accepts(port.annotation, value):
                    continue
                match = port.pattern.fullmatch(path)
                if match is None:
                    continue
                captures = match.groupdict()
                key = tuple(captures.get(group) for group in node.shared_groups)
                logger.info(
                    "route node=%s port=%s key=%s path=%s payload=%s",
                    node.name, port.name, safe_repr(key), path, safe_repr(value),
                )
                state = node.states.setdefault(
                    key, _KeyState({item.name: [] for item in node.ports})
                )
                state.next_sequence += 1
                state.buffers[port.name].append(
                    _Item(
                        value,
                        path,
                        captures,
                        parents if parents is not None else path,
                        state.next_sequence,
                    )
                )
                edge = node.edge_stats.setdefault((port.name, key), _EdgeStats())
                edge.calls += 1
                edge.last_call_time = time.time()
                if self._ready(node, state):
                    generation = self._arm_timer_locked(node, key, state)
                    if generation is not None:
                        ready_states.append((key, state, generation))
        for key, state, generation in ready_states:
            if generation is None:
                continue
            # A zero quiet period is synchronous and therefore preserves DFS.
            self._fire(node, key, state, generation, force=False)

    @staticmethod
    def _ready(node: _Node, state: _KeyState) -> bool:
        return all(port.optional or state.buffers[port.name] for port in node.ports) and any(
            not port.optional for port in node.ports
        )

    def _arm_timer_locked(self, node: _Node, key: tuple[Any, ...], state: _KeyState) -> int | None:
        now = time.monotonic()
        if not state.pending:
            state.pending = True
            state.max_generation += 1
            state.max_deadline = now + node.max_grace_period if node.max_grace_period is not None else None
            if state.max_deadline is not None:
                state.max_timer = Timer(
                    max(0.0, state.max_deadline - now),
                    self._timer_fire,
                    args=(node, key, state, state.max_generation, True),
                )
                state.max_timer.daemon = True
                state.max_timer.start()
        state.quiet_generation += 1
        generation = state.quiet_generation
        if state.quiet_timer:
            state.quiet_timer.cancel()
        delay = node.grace_period
        if delay <= 0:
            return generation
        state.quiet_timer = Timer(delay, self._timer_fire, args=(node, key, state, generation, False))
        state.quiet_timer.daemon = True
        state.quiet_timer.start()
        return None

    def _timer_fire(
        self, node: _Node, key: tuple[Any, ...], state: _KeyState, generation: int, force: bool
    ) -> None:
        with node.lock:
            valid = state.pending and (
                (force and generation == state.max_generation)
                or (not force and generation == state.quiet_generation)
            )
            if not valid or not self._ready(node, state):
                return
        self._fire(node, key, state, generation, force)

    def _fire(
        self, node: _Node, key: tuple[Any, ...], state: _KeyState, generation: int, force: bool
    ) -> None:
        with node.lock:
            if not state.pending or not self._ready(node, state):
                return
            if not force and generation != state.quiet_generation:
                return
            if force and generation != state.max_generation:
                return
            # Take this pending window off the timer path before invoking user
            # code. A callback publishing another input can start a new window.
            state.pending = False
            state.max_deadline = None
            if state.quiet_timer:
                state.quiet_timer.cancel()
                state.quiet_timer = None
            if state.max_timer:
                state.max_timer.cancel()
                state.max_timer = None
            combinations = self._combinations(node, state)

        for selected in combinations:
            if selected is None:
                continue
            args, captures, provenance, items = selected
            # Non-consuming joins must not re-emit a combination unless an
            # update occurred after its previous successful emission.
            maximum = max(
                (item.sequence for item in items if item is not None),
                default=0,
            )
            if not node.consume and maximum <= state.last_emitted_sequence:
                continue
            started = time.monotonic()
            try:
                token = _provenance.set(provenance)
                try:
                    result = node.function(*args)
                finally:
                    _provenance.reset(token)
            except Exception as exc:
                duration = time.monotonic() - started
                with node.lock:
                    node.stats.calls += 1
                    node.stats.failures += 1
                    node.stats.last_error = safe_repr(exc)
                    node.stats.total_duration += duration
                    node.stats.last_duration = duration
                    node.stats.last_call_time = time.time()
                logger.exception(
                    "push bus user function failed node=%s payload=%s",
                    node.name,
                    safe_repr(tuple(args) if len(args) != 1 else args[0]),
                )
                # Failed values remain available. Do not let one callback
                # failure prevent unrelated combinations/routes from running.
                continue

            duration = time.monotonic() - started
            with node.lock:
                node.stats.calls += 1
                node.stats.successes += 1
                node.stats.total_duration += duration
                node.stats.last_duration = duration
                node.stats.last_call_time = time.time()
                if not node.consume:
                    state.last_emitted_sequence = max(state.last_emitted_sequence, maximum)
                else:
                    for port, item in zip(node.ports, items):
                        if item is None:
                            continue
                        values = state.buffers[port.name]
                        state.buffers[port.name] = [candidate for candidate in values if candidate is not item]
            self._publish_result(node, result, captures, provenance)

    def _combinations(
        self, node: _Node, state: _KeyState
    ) -> list[tuple[tuple[Any, ...], dict[str, str], Any, tuple[_Item | None, ...]] | None]:
        choices: list[list[_Item | None]] = []
        for port in node.ports:
            values = state.buffers[port.name]
            choices.append(values if values else ([None] if port.optional else []))
        if any(not choice for choice in choices):
            return []
        answer = []
        for combination in itertools.product(*choices):
            captures: dict[str, str] = {}
            for item in combination:
                if item is not None:
                    captures.update(item.captures)
            args = tuple(item.value if item is not None else None for item in combination)
            provenance = {
                port.name: (item.provenance if item is not None else None)
                for port, item in zip(node.ports, combination)
            }
            answer.append((args, captures, provenance, combination))
        return answer

    def _publish_result(
        self, node: _Node, result: Any, captures: dict[str, str], provenance: Any
    ) -> None:
        if len(node.outputs) == 1:
            values = (result,)
        else:
            if not isinstance(result, tuple) or len(result) != len(node.outputs):
                raise TypeError(
                    f"{node.name} must return a tuple of {len(node.outputs)} values"
                )
            values = result
        for value, template in zip(values, node.outputs):
            output_path = template.format(**captures)
            self.publish(value, output_path, _parents=provenance)

    def statistics(self) -> dict[str, Any]:
        """Return a detached snapshot of node and concrete edge statistics."""
        with self._lock:
            nodes = tuple(self._nodes)
        node_snapshot: dict[str, Any] = {}
        edge_snapshot: dict[str, Any] = {}
        for node in nodes:
            with node.lock:
                stat = node.stats
                node_snapshot[node.name] = {
                    "calls": stat.calls,
                    "successes": stat.successes,
                    "failures": stat.failures,
                    "last_error": stat.last_error,
                    "total_duration": stat.total_duration,
                    "last_duration": stat.last_duration,
                    "last_call_time": stat.last_call_time,
                }
                for (port, key), edge in node.edge_stats.items():
                    edge_snapshot[f"{node.name}:{port}:{safe_repr(key)}"] = {
                        "node": node.name,
                        "port": port,
                        "key": key,
                        "calls": edge.calls,
                        "last_call_time": edge.last_call_time,
                    }
        return {"nodes": node_snapshot, "edges": edge_snapshot}

    stats = statistics
    get_stats = statistics


__all__ = ["PushBus", "Disposable", "current_provenance", "safe_repr"]
