# Internal Push Bus Plan

## Objective

Extend the small push-based bus into a multi-input/multi-output system while keeping user functions ordinary Python functions and preserving isolation between user-function failures and unrelated routes.

## Contract

- A subscription has named input paths and output templates.
- Regex capture groups shared by multiple inputs form the Join Key.
- Capture groups present on only one input are extraction fields for output templates, not Join Keys.
- If no capture group is shared, the Join Key is empty and matching uses the default Cartesian combination.
- Different capture values represent different buffered Join states.
- Required inputs must be present for a Join. Parameters annotated as accepting `None` may be absent and receive `None`.
- A single output may return a plain value. Multiple outputs return a tuple mapped positionally to output templates.
- `consume` controls whether values used by a successful Join are removed (`True`) or retained as latest values (`False`).
- `grace_period` debounces a ready Join: each update resets the quiet-period timer.
- `max_grace_period` bounds the total wait from the first ready update; expiry forces one emission.
- The implementation is Push-based. External callers own threading/asynchrony; internal state updates must remain safe.
- Default traversal is DFS in registration order; callers must not depend on ordering.
- User-function errors are isolated and logged. Internal bus errors are raised.
- Parent paths provide provenance. Multi-input calls retain provenance per input; a global trace/message ID is not required.
- Payload logging uses safe `repr()` with a 100-character limit and ellipsis truncation.

## Runtime state

Maintain a per-node, per-concrete-key buffer containing all unmatched values for each input port. Shared named capture groups, in declaration order, form the concrete Join Key; unnamed/positional groups are not Join Keys. A no-shared-key subscription uses one empty key and emits the Cartesian product of buffered input values. `consume=True` removes the values used by an emission; `consume=False` retains them, but a new emission requires a new input update. A successful emission resets the pending grace state. User-function failure does not consume values.

Optional parameters are mapped to input ports by function parameter order. `Optional[T]` and `T | None` inputs may be absent and receive `None`; at least one non-optional input must exist to trigger a call. Missing-input captures are unavailable to output templates.

Grace timers are per node and concrete key. The first ready update starts both the quiet timer and max timer. Every update resets only the quiet timer. Quiet expiry emits; max expiry forces one emission. Timer callbacks use an internal generation check so stale callbacks cannot emit twice. Unmatched buffers remain until consumed, explicitly disposed, or an explicitly requested cleanup operation. Dynamic statistics are enabled by default and keyed by concrete dynamic edges/keys.

## Statistics API

Expose a query function returning a snapshot suitable for an application API. Per concrete dynamic edge/key record call count and last-call time. Per node record user-function success/failure counts, last error, and user-function duration. Counts include attempted user-function calls; durations use a monotonic clock and seconds. Do not make UI or HTTP concerns part of this library.

## Scope exclusions

No default retry, persistence, queue/pull model, complete runtime generic-type system, or lifecycle/expiry framework. Support simple `Optional[T]` / `T | None` detection only; complex generics are not a goal.

## Execution

Implement the contract in the repository, preserving the existing one-input `subpub`/`pub` behavior where practical. Add focused tests for single-input compatibility, shared-key and extraction-only captures, Cartesian no-key joins, consume behavior, grace/max-grace, Optional inputs, tuple output, error isolation, provenance, safe logging, statistics, and concurrent state updates. Then run the available validation. This is one continuous execution without milestone gates.
