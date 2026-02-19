# Stage 1: Mark Live-Network Tests

**Effort:** Low (< 1 hour)
**Impact:** High — prevents CI flakes from network issues, makes `make test` reliable offline

## Problem

Several tests make live HTTP requests but are NOT marked as `@pytest.mark.slow`, so they run in every `make test` invocation. A flaky network or API downtime causes false failures.

## Affected Tests

### `packages/common/tests/test_requests.py`
- **Line 13** `test_get_metadata()` — calls `https://api.pdok.nl/brt/top10nl/download/v1_0/dataset`
- **Line 42** `test_download_as_str()` — calls `https://gist.githubusercontent.com/...AHN4.md5`
- **Line 48** `test_download_file()` — same GitHub gist URL
- **Line 53** `test_download_file_2()` — same GitHub gist URL

Already correctly marked as slow:
- Line 18 `test_download_link()` — `@pytest.mark.slow`
- Line 59 `test_download_extra()` — `@pytest.mark.slow`

### `packages/core/tests/test_assets_ahn.py`
- **Line 27** `test_download_ahn_index()` — calls `https://service.pdok.nl` (AHN index WFS)
- **Line 33** `test_download_ahn_index_geometry()` — same
- **Line 39** `test_get_checksums()` — calls `https://basisdata.nl/hwh-ahn/` checksum files
- **Line 54** `test_checksums_for_ahn()` — calls same checksum URLs (3 times, for AHN 3/4/5)
- **Line 69** `test_tile_index_ahn()` — calls AHN index + checksum URLs

Already correctly marked:
- Line 75 `test_laz_files_ahn3()` — `@pytest.mark.slow`
- Line 92 `test_laz_files_ahn4()` — `@pytest.mark.slow`
- Line 108 `test_laz_files_ahn5()` — `@pytest.mark.slow`

## Implementation

### Step 1: Add `network` marker to all conftest.py files

In each package's `conftest.py`, add to `pytest_configure`:
```python
config.addinivalue_line("markers", "network: mark test as requiring network access")
```

And in `pytest_collection_modifyitems`, add:
```python
if not config.getoption("--run-slow"):
    skip_network = pytest.mark.skip(reason="need --run-slow option to run")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip_network)
```

Note: reusing `--run-slow` to gate network tests avoids adding yet another CLI flag. Alternatively, add `--run-network` if you want finer control.

### Step 2: Mark the tests

In `packages/common/tests/test_requests.py`:
```python
@pytest.mark.network
def test_get_metadata(): ...

@pytest.mark.network
def test_download_as_str(): ...

@pytest.mark.network
def test_download_file(tmp_path): ...

@pytest.mark.network
def test_download_file_2(tmp_path): ...
```

In `packages/core/tests/test_assets_ahn.py`:
```python
@pytest.mark.network
def test_download_ahn_index(): ...

@pytest.mark.network
def test_download_ahn_index_geometry(): ...

@pytest.mark.network
@pytest.mark.parametrize(...)
def test_get_checksums(ahn_version): ...

@pytest.mark.network
def test_checksums_for_ahn(): ...

@pytest.mark.network
def test_tile_index_ahn(): ...
```

### Step 3: Update Makefile

In the `test_slow` target, ensure `--run-slow` is passed (it already is), which now also enables `network` tests.

Optionally add a dedicated target:
```makefile
test_network:
    docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest ... --run-slow -m network
```

## Files Modified

- `packages/common/tests/conftest.py` — add marker + skip logic
- `packages/core/tests/conftest.py` — add marker + skip logic
- `packages/common/tests/test_requests.py` — add `@pytest.mark.network` to 4 tests
- `packages/core/tests/test_assets_ahn.py` — add `@pytest.mark.network` to 5 tests

## Verification

```bash
# Should pass without network access (e.g., in airplane mode)
make test

# Should run all tests including network
make test_slow
```
