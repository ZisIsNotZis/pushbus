# Completion

**Status:** Completed on 2026-11-26.

**Accepted scope:** the dependency-free Python routing bus described in [PLAN.md](PLAN.md).

**Verification:** Run `python -m pytest -q` from a checkout with pytest installed. All 13 tests passed. They cover ordinary calls, named and positional ports, capture-key and Cartesian joins, consume behavior, grace timers, exceptions, provenance, statistics and concurrent updates.

**Source verified:** `d1d2a05068b80b2336fb3888e54240f931eba166`. This completion change adds documentation only.

**Limits:** Persistence, retries, a lifecycle framework and complex runtime generic typing are outside the library contract.
