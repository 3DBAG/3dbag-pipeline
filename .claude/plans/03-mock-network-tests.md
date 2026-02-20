# Stage 3: Add Mocked Variants of Network Tests

**Effort:** Medium (2-3 hours)
**Impact:** High — ensures core parsing/processing logic is tested without network dependency

## Problem

After Stage 1 marks network tests, the "fast" test suite loses coverage for important parsing and processing logic that's currently only tested via live HTTP calls. We need mocked versions that test the same logic using fixture data.

## Tests to Add Mocked Variants For

### 3.1 `test_requests.py` — Mock HTTP responses

**Current tests and what they actually validate:**
- `test_get_metadata()` — validates JSON response parsing from PDOK API
- `test_download_as_str()` — validates string download and content parsing
- `test_download_file()` / `test_download_file_2()` — validates file download and path handling

**Implementation using `responses` library:**

Add `responses` to dev dependencies:
```bash
uv add -p packages/common --dev responses
```

Create `packages/common/tests/test_requests_mocked.py`:

```python
"""Mocked variants of network tests — run without network access."""
import responses

from bag3d.common.utils.requests import (
    get_metadata,
    download_as_str,
    download_file,
)

MOCK_PDOK_RESPONSE = {
    "_links": {"self": {"href": "..."}},
    "tilesPerFormat": [{"format": "gml", "tiles": []}],
}

MOCK_MD5_CONTENT = "56c731a1814dd73c79a0a5347f8a04c7  C_01CZ1.LAZ\naaa111  C_50CN2.LAZ\n"


@responses.activate
def test_get_metadata_mocked():
    responses.add(
        responses.GET,
        "https://api.pdok.nl/brt/top10nl/download/v1_0/dataset",
        json=MOCK_PDOK_RESPONSE,
        status=200,
    )
    res = get_metadata("https://api.pdok.nl/brt/top10nl/download/v1_0/dataset")
    assert res is not None


@responses.activate
def test_download_as_str_mocked():
    url = "https://example.com/AHN4.md5"
    responses.add(responses.GET, url, body=MOCK_MD5_CONTENT, status=200)
    res = download_as_str(url=url)
    assert res.split("\n", 1)[0] == "56c731a1814dd73c79a0a5347f8a04c7  C_01CZ1.LAZ"


@responses.activate
def test_download_file_mocked(tmp_path):
    url = "https://example.com/AHN4.md5"
    responses.add(responses.GET, url, body=MOCK_MD5_CONTENT, status=200)
    res = download_file(url=url, target_path=tmp_path / "test.md5")
    assert res == tmp_path / "test.md5"
    assert res.read_text().startswith("56c731a1814dd73c79a0a5347f8a04c7")
```

### 3.2 `test_assets_ahn.py` — Mock AHN index and checksum downloads

**Current tests and what they actually validate:**
- `test_download_ahn_index()` — validates WFS response parsing (1407 tiles)
- `test_get_checksums()` — validates MD5/SHA256 file parsing
- `test_tile_index_ahn()` — validates combined index+URL building

**Implementation using fixtures + `unittest.mock.patch`:**

The `test_ahn_automation.py` file already demonstrates this pattern beautifully (lines 248-249):
```python
with (
    patch("bag3d.core.sensors.get_checksums", return_value=CHECKSUMS_V1),
    patch("bag3d.core.sensors.download_ahn_index", return_value=TILE_INDEX),
):
```

Create `packages/core/tests/test_assets_ahn_mocked.py`:

```python
"""Mocked variants of AHN download tests — run without network access."""
from unittest.mock import patch
from bag3d.core.assets.ahn.download import get_checksums, tile_index_ahn

# Fixture data: a small subset of the real AHN index
MOCK_AHN_INDEX = {
    "01cz1": None,
    "32bz1": None,
    "50cn2": None,
}

MOCK_AHN_INDEX_WITH_GEOM = {
    "01cz1": {"type": "Polygon", "coordinates": [[[140000, 600000], [140000, 606250], [145000, 606250], [145000, 600000], [140000, 600000]]]},
    "32bz1": {"type": "Polygon", "coordinates": [[[160000, 380000], [160000, 386250], [165000, 386250], [165000, 380000], [160000, 380000]]]},
}

MOCK_MD5_TEXT = "aaa111  C_01CZ1.LAZ\nbbb222  C_32BZ1.LAZ\n"


def test_get_checksums_parsing():
    """Test that get_checksums correctly parses MD5 text format."""
    with patch("bag3d.core.assets.ahn.download.download_as_str", return_value=MOCK_MD5_TEXT):
        checksums = get_checksums("https://example.com/checksums", ahn_version=3)
    assert checksums == {"C_01CZ1.LAZ": "aaa111", "C_32BZ1.LAZ": "bbb222"}


def test_tile_index_ahn_structure():
    """Test that tile_index_ahn builds correct structure from components."""
    # Patch both the index download and checksum downloads
    with (
        patch("bag3d.core.assets.ahn.download.download_ahn_index", return_value=MOCK_AHN_INDEX_WITH_GEOM),
        # ... patch URL building
    ):
        result = tile_index_ahn()
    assert isinstance(result, dict)
    # Verify structure has expected keys
```

Note: the exact patches depend on the internal implementation of `tile_index_ahn`. Read `packages/core/src/bag3d/core/assets/ahn/download.py` to determine the right patch targets.

### 3.3 `test_geodata.py` — already mostly local

`test_info_exes`, `test_info_data`, `test_ogr2postgres` use local test data files and GDAL executables. These don't need mocking. Only `test_parse_ogrinfo` and `test_geojson_poly_to_wkt` are pure-logic tests that don't need any external resources.

No changes needed for this file.

## Files Modified

- **New:** `packages/common/tests/test_requests_mocked.py`
- **New:** `packages/core/tests/test_assets_ahn_mocked.py`
- `packages/common/pyproject.toml` — add `responses` to dev dependencies

## Verification

```bash
# Run only the new mocked tests (should work without network/Docker DB)
pytest packages/common/tests/test_requests_mocked.py -v
pytest packages/core/tests/test_assets_ahn_mocked.py -v

# Full test suite still passes
make test
```
