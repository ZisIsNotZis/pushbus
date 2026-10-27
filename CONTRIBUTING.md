# Contributing to pushbus

Thanks for taking a look. This is a small, deliberately narrow library: one module, one contract, no dependencies.

## Scope

The behaviour of the bus is specified in [`PLAN.md`](PLAN.md). Changes belong there first — a pull request that alters join keys, buffering, grace timers, provenance, or statistics without a matching contract change will be asked to add one.

Out of scope by design (see "Scope exclusions" in [`PLAN.md`](PLAN.md)): retry policies, persistence, a queue or pull model, a general runtime type system, lifecycle/expiry frameworks, and any UI, HTTP, or framework integration. The bus also does not own threading or scheduling for callers.

## Running the checks

```sh
python -m pytest
```

There is no linter or type-check configuration in the repository; the test suite is the check. Please run the whole suite before opening a pull request.

## Pull requests

- One behaviour per pull request, with a test that fails before the change and passes after it.
- Keep public API additions consistent with the existing style: paths are keyword-optional, reserved options start with `_`, and user functions stay ordinary Python functions.
- Do not add third-party runtime dependencies. The library is standard library only.
- Keep the docstring and documentation honest: if a claim is not true of the code, it is a bug.
- Update [`PLAN.md`](PLAN.md) or the README when the contract or the user-visible surface changes.

## Issues

Bug reports are most useful with a minimal reproduction: the subscription, the `publish` calls, and what you expected instead of what happened. For timing questions (grace periods, ordering), say which Python version you ran.

## License

Contributions are accepted under the AGPL-3.0, the license of this repository. See [`LICENSE.md`](LICENSE.md).
