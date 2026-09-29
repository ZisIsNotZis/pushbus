# pushbus

> **Status: closed (milestone, 2026-09-29).** A complete, tested library.
> No further development is planned unless the project's inputs or goals change.

A Python push-based routing bus that joins values from named input paths, calls ordinary user functions, and emits values through output templates. The intended contract—including join keys, optional inputs, buffering, consume behavior, grace timers, provenance, error isolation, and statistics—is described in [`PLAN.md`](PLAN.md).

## Development

Run the tests with:

```sh
python -m pytest
```

The implementation is in [`pushbus.py`](pushbus.py); tests are in [`test_pushbus.py`](test_pushbus.py). External callers own threading and asynchronous execution; the bus is not a persistent queue or pull-based runtime. See [`docs/project-status.md`](docs/project-status.md).