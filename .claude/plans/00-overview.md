# Test Improvement Plan — Overview

## Problem Statement

The 3dbag-pipeline test suite suffers from three interconnected problems:

1. **Slow tests** — Many "fast" tests make live HTTP requests to external APIs (PDOK, basisdata.nl, GitHub gists), adding seconds of latency and risking CI flakes from network issues.

2. **Brittle asset chain tests** — Tests in `floors_estimation` and `core/input` depend on database tables created by *previous* tests. If test order changes, a test fails midway, or you want to run a single test in isolation, the chain breaks.

3. **Duplicated infrastructure** — All 4 packages copy-paste ~60 lines of identical conftest boilerplate (env var reading, marker registration, skip logic, database fixture).

4. **Monolithic test data** — A single `test_data_v14.zip` blob serves all packages. Updating test data for one package requires rebuilding the entire zip.

5. **No middle-ground tests** — The only way to test multi-asset workflows is full integration tests requiring external tool binaries (`@pytest.mark.needs_tools`). There's no lightweight way to test asset graph wiring.

## Staged Plan

| Stage | File | Effort | Impact | Summary |
|-------|------|--------|--------|---------|
| 1 | `01-mark-network-tests.md` | Low | High | Mark live-network tests, add `@pytest.mark.network` |
| 2 | `02-extract-conftest-boilerplate.md` | Low | Medium | DRY up shared conftest code into a pytest plugin |
| 3 | `03-mock-network-tests.md` | Medium | High | Add mocked variants of HTTP-calling tests |
| 4 | `04-break-sequential-chains.md` | Medium | High | Decouple sequential DB-dependent test chains |
| 5 | `05-database-isolation.md` | Medium | High | Add transaction rollback fixtures for test isolation |
| 6 | `06-graph-wiring-tests.md` | High | High | Add lightweight `materialize()` tests for asset graphs |

## What's Already Good

- `test_automation_conditions.py` and `test_ahn_automation.py` — exemplary Dagster tests using `evaluate_automation_conditions()`, `DagsterInstance.ephemeral()`, `ResourceDefinition.mock_resource()`, and `unittest.mock.patch`. These require no database, no network, no tools.
- `test_code_location.py` in all 3 packages — `Definitions.validate_loadable()` catches import/wiring errors cheaply.
- `MockAssetIOManager` in `core/tests/conftest.py` — good pattern for stubbing upstream assets in integration tests.
- `mock_preprocessed_features` / `mock_inferenced_floors` in `floors_estimation/tests/conftest.py` — in-memory DataFrame fixtures that avoid DB dependency.

These patterns should be extended to more tests across the codebase.
