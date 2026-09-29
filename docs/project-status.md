# pushbus project status

## Classification

Closed library: a push-based routing bus that joins values from named input
paths, calls ordinary user functions, and emits values through output templates.

## Status

Closed as a milestone (2026-09-29). The contract described in
[`../PLAN.md`](../PLAN.md) is implemented and tested; no further development is
planned unless the project's inputs or goals change.

## Evidence

- Implementation: [`../pushbus.py`](../pushbus.py).
- Contract: [`../PLAN.md`](../PLAN.md) — join keys, optional inputs, buffering,
  consume behavior, grace timers, provenance, error isolation, statistics.
- Tests: [`../test_pushbus.py`](../test_pushbus.py) — `python -m pytest`
  (13 passed on 2026-09-29).

## Boundaries

External callers own threading and asynchronous execution. The bus is not a
persistent queue or a pull-based runtime, and there is no packaged release or
CI.
