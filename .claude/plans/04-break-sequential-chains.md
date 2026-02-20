# Stage 4: Break Sequential Test Chains

**Effort:** Medium (2-4 hours)
**Impact:** High — enables running individual tests in isolation, faster iterative development

## Problem

Several test files contain tests that MUST run in a specific order because each test creates database tables that subsequent tests depend on. This means:
- You cannot run a single test with `pytest -k test_name`
- A failure in an early test cascades to all later tests
- Adding a new test requires understanding the entire chain

### The `floors_estimation` chain (worst offender)

In `packages/floors_estimation/tests/test_floors_estimation.py`, the implicit execution order is:

```
test_bag3d_features        → creates floors_estimation.building_features_bag3d
test_external_features     → creates floors_estimation.building_features_external
test_all_features          → reads both tables above, creates floors_estimation.building_features_all
test_preprocessed_features → reads building_features_all, returns DataFrame
test_inferenced_floors     → uses mock_preprocessed_features (independent!)
test_predictions_table     → uses mock_inferenced_floors (independent!)
test_save_cjfiles          → uses mock_inferenced_floors (independent!)
```

The first 4 tests form a chain. The last 3 are already decoupled via mock fixtures.

### The `core/input` chain

In `packages/core/tests/test_assets_input.py`:

```
test_bag_kas_warenhuis → needs lvbag.pandactueelbestaand AND top10nl.gebouw (from test data)
test_bag_bag_overlap   → needs lvbag.pandactueelbestaand (from test data)
```

These aren't chained to each other, but they depend on pre-loaded database tables from the test data zip.

## Implementation

### 4.1 Fix `floors_estimation` chain with module-scoped fixtures

Replace the implicit chain with explicit fixtures that materialize prerequisites once per module:

In `packages/floors_estimation/tests/conftest.py`, add:

```python
@pytest.fixture(scope="module")
def materialized_bag3d_features(database, mock_features_file_index):
    """Materialize bag3d_features once for the test module."""
    from bag3d.floors_estimation.assets.floors_estimation import bag3d_features, FloorsEstimationConfig
    result = bag3d_features(FloorsEstimationConfig(), mock_features_file_index, database)
    return result.value


@pytest.fixture(scope="module")
def materialized_external_features(database):
    """Materialize external_features once for the test module."""
    from bag3d.floors_estimation.assets.floors_estimation import external_features
    result = external_features(database)
    return result.value


@pytest.fixture(scope="module")
def materialized_all_features(database, materialized_bag3d_features, materialized_external_features):
    """Materialize all_features once for the test module."""
    from bag3d.floors_estimation.assets.floors_estimation import all_features
    result = all_features(materialized_external_features, materialized_bag3d_features, database)
    return result.value
```

Then rewrite the tests to use these fixtures explicitly:

```python
def test_bag3d_features(materialized_bag3d_features, database):
    """Test that bag3d_features creates the expected table."""
    building_feature_table = PostgresTableIdentifier("floors_estimation", "building_features_bag3d")
    assert table_exists(database, building_feature_table) is True


def test_external_features(materialized_external_features, database):
    """Test that external_features creates the expected table."""
    external_features_table = PostgresTableIdentifier("floors_estimation", "building_features_external")
    assert table_exists(database, external_features_table) is True


def test_all_features(materialized_all_features, database):
    """Test that all_features creates the expected table."""
    all_features_table = PostgresTableIdentifier("floors_estimation", "building_features_all")
    assert table_exists(database, all_features_table) is True


def test_preprocessed_features(materialized_all_features, database):
    """Test that preprocessed_features works with materialized all_features."""
    all_features_table = PostgresTableIdentifier("floors_estimation", "building_features_all")
    data = preprocessed_features(all_features_table, database)
    assert data is not None
    assert data.shape[0] == 6
```

**Key benefit:** Now `pytest -k test_preprocessed_features` works — pytest will automatically materialize all prerequisite fixtures.

### 4.2 Make `core/input` tests self-contained

The input tests depend on `lvbag.pandactueelbestaand` and `top10nl.gebouw` existing in the database. These are loaded by the test data setup. To make tests self-contained:

**Option A (recommended): Add a fixture that verifies prerequisites exist**

```python
@pytest.fixture
def require_source_tables(database):
    """Skip test if source data tables don't exist."""
    required = [
        PostgresTableIdentifier("lvbag", "pandactueelbestaand"),
        PostgresTableIdentifier("top10nl", "gebouw"),
    ]
    for tbl in required:
        if not table_exists(database, tbl):
            pytest.skip(f"Required table {tbl} not found — run make download and load test data first")
```

Then use in tests:
```python
def test_bag_kas_warenhuis(database, require_source_tables):
    ...
```

**Option B: Create minimal fixtures with synthetic data**

For pure unit testing, create fixtures that insert 5-10 rows into temporary tables:

```python
@pytest.fixture
def synthetic_pandactueelbestaand(database):
    """Create a minimal pandactueelbestaand table for testing."""
    tbl = PostgresTableIdentifier("test_lvbag", "pandactueelbestaand")
    # Create table with minimal schema
    database.connect.send_query(SQL("""
        CREATE SCHEMA IF NOT EXISTS test_lvbag;
        CREATE TABLE test_lvbag.pandactueelbestaand AS
        SELECT * FROM lvbag.pandactueelbestaand LIMIT 10;
    """))
    yield tbl
    drop_table(database, tbl, get_dagster_logger())
    database.connect.send_query(SQL("DROP SCHEMA IF EXISTS test_lvbag CASCADE;"))
```

This option requires modifying the asset functions to accept table identifiers as parameters (which they already do — `bag_kas_warenhuis` takes `bag_pandactueelbestaand` and `top10nl_gebouw` as inputs).

### 4.3 Document test execution requirements

Add a note to the relevant test files explaining that they require test data:

```python
"""Tests for input assets.

Prerequisites:
    - Database must have lvbag.pandactueelbestaand table loaded (via make download + test data setup)
    - Database must have top10nl.gebouw table loaded
"""
```

## Files Modified

- `packages/floors_estimation/tests/conftest.py` — add module-scoped materialization fixtures
- `packages/floors_estimation/tests/test_floors_estimation.py` — rewrite to use explicit fixtures
- `packages/core/tests/test_assets_input.py` — add `require_source_tables` fixture or skip logic

## Verification

```bash
# Each test should be runnable independently
docker compose -p bag3d-dev exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/test_floors_estimation.py::test_preprocessed_features -v

# The full module still passes
docker compose -p bag3d-dev exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/test_floors_estimation.py -v

# Full suite still passes
make test
```
