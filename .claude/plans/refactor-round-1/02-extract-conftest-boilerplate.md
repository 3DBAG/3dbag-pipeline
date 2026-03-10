# Stage 2: Extract Conftest Boilerplate into Shared Plugin

**Effort:** Low-Medium (1-2 hours)
**Impact:** Medium — reduces maintenance burden, ensures consistent test infrastructure

## Problem

All 4 package conftest.py files duplicate ~60 lines of identical code:
- Env var reading (`BAG3D_PG_HOST`, `BAG3D_PG_PORT`, etc.) — 6 lines
- `pytest_addoption` — `--run-slow`, `--run-all` — 10 lines
- `pytest_configure` — marker registration — 6 lines
- `pytest_collection_modifyitems` — skip logic — 15 lines
- `database` fixture — 5 lines
- `test_data_dir` fixture — 3 lines

When a new marker or option is added, it must be copy-pasted to all 4 files (and currently `--run-deploy` only exists in core).

## Implementation

### Step 1: Create shared pytest plugin module

Create `packages/common/src/bag3d/common/testing/conftest_plugin.py`:

```python
"""Shared pytest plugin for all bag3d test packages.

Register via conftest.py:
    pytest_plugins = ["bag3d.common.testing.conftest_plugin"]
"""
import os
from pathlib import Path

import pytest
from bag3d.common.resources.database import DatabaseResource


LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = os.getenv("BAG3D_PG_HOST")
PORT = int(os.getenv("BAG3D_PG_PORT", "5432"))
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")


def pytest_addoption(parser):
    parser.addoption("--run-slow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--run-all", action="store_true", default=False, help="run all tests, including the ones that needs local builds of tools")
    parser.addoption("--run-deploy", action="store_true", default=False, help="run deployment tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "needs_tools: mark test as needing local builds of tools")
    config.addinivalue_line("markers", "needs_deploy: mark test as needing the dockerized deployment setup")
    config.addinivalue_line("markers", "network: mark test as requiring network access")


def pytest_collection_modifyitems(config, items):
    marker_option_pairs = [
        ("slow", "--run-slow", "need --run-slow option to run"),
        ("network", "--run-slow", "need --run-slow option to run"),
        ("needs_tools", "--run-all", "needs the --run-all option to run"),
        ("needs_deploy", "--run-deploy", "needs the --run-deploy option to run"),
    ]
    for marker_name, option, reason in marker_option_pairs:
        if not config.getoption(option):
            skip_marker = pytest.mark.skip(reason=reason)
            for item in items:
                if marker_name in item.keywords:
                    item.add_marker(skip_marker)


@pytest.fixture
def database():
    db = DatabaseResource(host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME)
    yield db


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)
```

### Step 2: Create `__init__.py`

Create `packages/common/src/bag3d/common/testing/__init__.py` (empty file).

### Step 3: Simplify each package's conftest.py

Each conftest.py becomes much shorter. For example, `packages/core/tests/conftest.py`:

```python
# Register the shared plugin
pytest_plugins = ["bag3d.common.testing.conftest_plugin"]

# Package-specific fixtures only
from bag3d.common.resources.files import FileStoreResource
# ... rest of core-specific fixtures (gdal, validation, mock assets, etc.)

@pytest.fixture
def file_store(tmp_path):
    yield FileStoreResource(data_dir=str(tmp_path))

# ... other core-specific fixtures
```

Remove from each conftest.py:
- `pytest_addoption`, `pytest_configure`, `pytest_collection_modifyitems` (provided by plugin)
- `database` fixture (provided by plugin)
- `test_data_dir` fixture (provided by plugin)
- Env var declarations at module level (provided by plugin)

### Step 4: Handle the `--run-deploy` option

Currently only `core/tests/conftest.py` has `--run-deploy`. By moving it to the shared plugin, all packages get it, which is harmless (the marker is simply never used in other packages).

## Files Modified

- **New:** `packages/common/src/bag3d/common/testing/__init__.py`
- **New:** `packages/common/src/bag3d/common/testing/conftest_plugin.py`
- `packages/common/tests/conftest.py` — remove boilerplate, add `pytest_plugins` line
- `packages/core/tests/conftest.py` — remove boilerplate, add `pytest_plugins` line
- `packages/floors_estimation/tests/conftest.py` — remove boilerplate, add `pytest_plugins` line
- `packages/party_walls/tests/conftest.py` — remove boilerplate, add `pytest_plugins` line

## Verification

```bash
# Run each package's tests independently — all should still pass
make test

# Verify markers work
docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/ --co -m slow
docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/ --co -m network
```
