# Stage 5: Database Test Isolation

**Effort:** Medium (2-3 hours)
**Impact:** High — prevents test pollution, enables parallel test execution

## Problem

Multiple tests write to the same database without cleanup guarantees. If a test fails midway after creating a table but before dropping it, the stale table may cause subsequent tests or re-runs to behave differently. Examples:

- `packages/common/tests/test_database.py:24` — `test_drop_table` creates `public.non_existing_table`
- `packages/common/tests/test_database.py:75` — `test_postgrestable_from_query` creates `public.test_table` and has explicit "clean up if table exists from previous run" code (line 82-83), proving the problem exists
- `packages/core/tests/test_assets_bag.py:21` — `test_load_bag_layer` creates then drops `lvbag.test_ligplaats`
- `packages/core/tests/test_assets_bag.py:56` — `test_stage_bag_layer` creates then drops `stage_lvbag.ligplaats`
- `packages/floors_estimation/tests/test_floors_estimation.py` — creates tables in `floors_estimation` schema that persist

## Implementation

### 5.1 Create a transaction-based database fixture

Add to the shared testing plugin (`packages/common/src/bag3d/common/testing/conftest_plugin.py`):

```python
@pytest.fixture
def database_transaction(database):
    """Database resource that rolls back all changes after the test.

    Use this instead of `database` for tests that write to the database
    but shouldn't leave permanent state.
    """
    # Get the underlying psycopg connection
    conn = database.connect._conn  # adjust based on actual DatabaseResource internals
    conn.autocommit = False

    yield database

    conn.rollback()
    conn.autocommit = True
```

**Important caveat:** This requires understanding how `DatabaseResource.connect` manages its connection. The fixture needs to wrap the same connection that the asset/function under test uses. If `DatabaseResource` creates new connections per query, this approach won't work and we need an alternative.

### 5.2 Alternative: Schema-per-test isolation

If transaction rollback isn't feasible due to how the DB resource manages connections, use unique schemas per test:

```python
@pytest.fixture
def isolated_schema(database):
    """Create a unique schema for this test and drop it after."""
    import uuid
    schema_name = f"test_{uuid.uuid4().hex[:8]}"
    database.connect.send_query(SQL("CREATE SCHEMA {}").format(Identifier(schema_name)))
    yield schema_name
    database.connect.send_query(SQL("DROP SCHEMA {} CASCADE").format(Identifier(schema_name)))
```

Tests that create tables use the isolated schema:
```python
def test_load_bag_layer(database, isolated_schema, ...):
    test_table = PostgresTableIdentifier(isolated_schema, "test_ligplaats")
    res = load_bag_layer(production_db=database, ..., new_table=test_table, ...)
    assert res is True
    # No manual cleanup needed — schema dropped by fixture
```

### 5.3 Add cleanup guards to existing tests

For tests that can't easily be refactored, add `try/finally` cleanup patterns. Some tests already do this (`test_assets_deploy.py:44`), but others don't:

```python
def test_bag_kas_warenhuis(database):
    logger = get_dagster_logger()
    new_table = PostgresTableIdentifier("reconstruction_input", "bag_kas_warenhuis")
    try:
        res = intermediary.bag_kas_warenhuis(...)
        assert isinstance(res.value, PostgresTableIdentifier)
    finally:
        drop_table(database, new_table, logger)
```

### 5.4 Add database state validation fixture

A fixture that runs before each test to verify expected database state:

```python
@pytest.fixture(autouse=True)
def _verify_db_clean_state(database, request):
    """Log a warning if leftover test tables exist."""
    # Only check for test modules that write to DB
    if "test_assets" not in request.node.nodeid:
        return
    # Check for common leftover tables
    known_test_tables = [
        PostgresTableIdentifier("public", "non_existing_table"),
        PostgresTableIdentifier("public", "test_table"),
    ]
    for tbl in known_test_tables:
        if table_exists(database, tbl):
            import warnings
            warnings.warn(f"Leftover table {tbl} found before test — cleaning up")
            drop_table(database, tbl, get_dagster_logger())
```

## Investigation Needed

Before implementing, we need to check how `DatabaseResource.connect` manages connections:

```bash
# Check the implementation
grep -n "class DatabaseConnection" packages/common/src/bag3d/common/resources/database.py
grep -n "def send_query" packages/common/src/bag3d/common/resources/database.py
```

If the connection uses autocommit (common with psycopg3), we need approach 5.2 (schema isolation) instead of 5.1 (transaction rollback).

## Files Modified

- `packages/common/src/bag3d/common/testing/conftest_plugin.py` — add `database_transaction` or `isolated_schema` fixture
- `packages/common/tests/test_database.py` — use isolation fixture
- `packages/core/tests/test_assets_bag.py` — use isolation fixture
- `packages/core/tests/test_assets_input.py` — add cleanup guards
- `packages/floors_estimation/tests/test_floors_estimation.py` — use isolation fixture where appropriate

## Verification

```bash
# Run tests twice in a row — second run should produce identical results
make test && make test

# Run a single test that creates tables — verify no leftover state
docker compose -p bag3d-dev exec bag3d-core pytest .../test_database.py::test_drop_table -v
docker compose -p bag3d-dev exec data-postgresql psql -U bag3d_user -d bag3d -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'non_existing_table';"
# Should return 0 rows
```
