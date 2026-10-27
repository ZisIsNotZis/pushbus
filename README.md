**English** | [简体中文](README.zh-CN.md)

<p align="center"><img src="assets/logo.svg" alt="pushbus" width="200" height="40"></p>

# pushbus

A push-based routing bus for Python: values arrive on named paths, get joined by regex capture keys, are handed to ordinary functions, and are re-published through output templates.

**It turns a stream of unrelated callbacks into a dataflow graph — buffer inputs until they pair up, call your function once per pair, route the result onward — with no broker, no queue server, no framework, and no dependencies.**

`pushbus` is one module. You decorate plain Python functions; the bus owns the buffering, the joining, the timers, and the counters. Threading and async remain yours: the bus never starts a scheduler of its own.

## Quick start

```sh
git clone https://github.com/ZisIsNotZis/pushbus
cd pushbus
python -m pytest
```

That is the whole installation: `pushbus.py` is the library, `pytest` is the only test-time dependency. Requires Python 3.10 or newer (the bus inspects `types.UnionType` to detect `T | None` ports).

Then:

```python
from pushbus import PushBus

bus = PushBus()

@bus.subscribe(
    a=r"temp/(?P<room>\w+)",
    b=r"hum/(?P<room>\w+)",
    _outputs="comfort/{room}",
)
def comfort(temp: float, hum: float):
    return temp + hum

@bus.subscribe(_inputs=r"comfort/(?P<room>\w+)", _outputs="log")
def show(value: float):
    print(f"comfort = {value}")

bus.publish(21.5, "temp/kitchen")   # buffered: no partner yet
bus.publish(40.0, "hum/kitchen")    # same room -> join fires -> show(61.5)
```

Inputs that share a named capture group (`room`) form a join key, so `temp/kitchen` only pairs with `hum/kitchen`. Captures that appear on a single input (`room` above) are still usable in output templates.

## What it can do now

- **Path routing** — every input path is a full-match regex; named captures are extracted per value.
- **Joins by key** — capture groups shared by all inputs define the key; each concrete key keeps its own buffer, so rooms, users, or devices never cross-talk.
- **Extraction-only captures** — a group present on one input is not part of the key but is available to output templates such as `"comfort/{room}"`.
- **Cartesian fallback** — with no shared capture group, one key is used and the bus emits the product of the buffered values.
- **Three input forms** — positional paths, `_inputs=` as a single path or sequence, or `_inputs=` as a mapping of named ports to paths.
- **Multiple outputs** — return a tuple and it is mapped positionally onto the output templates; a single output returns a plain value.
- **Optional ports** — a parameter annotated `Optional[T]` or `T | None` may be absent and receives `None`; at least one required input must be present to trigger a call.
- **Consume or retain** — `_consume=True` (default) removes the values a successful join used; `_consume=False` keeps them as latest values, but a fresh emission still needs a new update.
- **Debounce and hard cap** — `_grace_period` restarts a quiet timer on every update; `_max_grace_period` bounds the total wait and forces one emission. Timers are generation-guarded so a stale callback can never fire twice.
- **Error isolation** — an exception in your function is logged and counted, leaves its values buffered, and does not stop other routes; internal errors (such as a bad output template) are raised to the caller.
- **Provenance** — `current_provenance()` returns the per-input paths that produced the current call.
- **Statistics** — `bus.statistics()` snapshots per-node calls, successes, failures, last error, and durations, plus per-port-per-key edge call counts and timestamps.
- **Safe logging** — values are logged via `safe_repr()`: bounded to 100 characters, and objects with a broken `__repr__` cannot break dispatch.
- **Disposal** — every subscription returns a `Disposable`; use `fn.dispose()` / `fn.subscription` to unregister and cancel its timers.
- **Thread safety** — per-node re-entrant locks; `publish` takes a registration snapshot, so a route registered during a publish starts on the next one.

## Architecture

The library is a single dispatcher around a per-subscription state machine.

`PushBus.publish(value, path)` logs the value, snapshots the registered nodes, and offers the value to each one in registration order. A node matches the path against each of its input ports; a match appends the value to that port's buffer under the concrete join key. When every non-optional port has a value, the node becomes ready: the grace timers are armed, and on quiet expiry (immediately when `_grace_period` is `0`) the node takes all buffered combinations, calls the user function once per combination, updates the counters, and republishes each returned value through its output template — which re-enters `publish` and lets routes chain.

```
pushbus.py          the whole library
  PushBus           registration, publish, per-node dispatch, statistics
  _Node / _Port     a subscription and its compiled input paths
  _KeyState         buffers, generation counters, quiet and max timers per key
  _Item             one buffered value with path, captures, provenance, sequence
  Disposable        idempotent unsubscription handle
test_pushbus.py     behaviour tests for the contract
PLAN.md             the contract: join keys, grace, provenance, statistics
docs/project-status.md  milestone status and evidence
```

Zero quiet period means synchronous depth-first traversal (`a → b → c` completes before the next route from `a` runs); a non-zero grace period moves emission onto daemon `threading.Timer` threads. Either way the buffered state is guarded per node and the design assumes external callers own execution and persistence — it is not a queue and has no pull interface.

## How to develop

```sh
python -m pytest                          # full suite
python -m pytest -k grace                 # single behaviour
python -m pytest --log-cli-level=INFO      # watch routing decisions
```

There is no packaging metadata, linter config, or type-check config in the repository; the checks that exist are the test suite. Tests use `pytest` features (`caplog`, `pytest.raises`), so keep new tests in the same style.

## How to contribute

See [CONTRIBUTING.md](CONTRIBUTING.md). In short: open an issue describing the behaviour you want, keep the change inside the contract in [`PLAN.md`](PLAN.md), and add a test that fails before your fix.

## Roadmap

**Implemented** — the full contract in [`PLAN.md`](PLAN.md): multi-input joins over shared regex capture keys, extraction-only captures, cartesian no-key joins, optional ports, consume and retain modes, quiet and maximum grace periods, tuple outputs, per-key provenance, user-error isolation, bounded logging, dynamic statistics, thread-safe state, and the single-input convenience path.

**Planned** — nothing. The repository is closed as a milestone (2026-09-29); [`docs/project-status.md`](docs/project-status.md) records the status and its evidence.

The future is deliberately narrow: if the inputs or goals change, work would start from the scope exclusions already written in [`PLAN.md`](PLAN.md) — no retry policy, no persistence, no queue or pull model, no general runtime type system, no lifecycle framework. Until then the bus stays small enough to read in one sitting, which is the point.

## License

AGPL-3.0. See [LICENSE.md](LICENSE.md). Copyright (C) ZisIsNotZis.
