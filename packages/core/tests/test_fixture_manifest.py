import json

import pytest

from bag3d.core.assets.fixture_adapters import validate_fixture_manifest
from bag3d.core.fixture_contract import VECTOR_PROCESSING


def snapshot(tmp_path, version):
    sources = {}
    files = {}
    for name in ("bag", "bgt", "top10nl", "cbs_key_figures", "cbs_buurtkaart"):
        (tmp_path / name).write_bytes(b"fixture")
        sources[name] = {"path": name}
        files[name] = {"size": 7}
    manifest = {
        "fixture_version": version,
        "aoi": {
            "wkt": "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
            "minx": 0,
            "miny": 0,
            "maxx": 10,
            "maxy": 10,
        },
        "clipping": {
            "vectors": "gdal" if str(version) == "2" else VECTOR_PROCESSING,
            "pointclouds": "las2las_keep_xy",
        },
        "validation": {
            "buffer_metres": 10.0,
            "extents": {
                "vectors": {
                    "bgt": [[1, 1, 9, 9]],
                    "top10nl": [[1, 1, 9, 9]],
                    "bag": [[1, 1, 9, 9]],
                    "cbs_buurtkaart": [[1, 1, 9, 9]],
                },
                "pointclouds": {"tile.laz": [1, 1, 9, 9]},
            },
        },
        "sources": sources,
        "files": files,
        "ahn": {
            "indexes": {"3": {}, "6": {}},
            "checksums": {str(v): {} for v in (3, 4, 5, 6)},
            "partitions": {},
        },
    }
    return manifest


def validate(tmp_path, manifest):
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return validate_fixture_manifest(tmp_path)


@pytest.mark.parametrize("version", ["2", 2, "3", 3])
def test_manifest_versions_supported(tmp_path, version):
    assert (
        validate(tmp_path, snapshot(tmp_path, version)).value["fixture_version"]
        == version
    )


@pytest.mark.parametrize("version", ["1", 4, None])
def test_manifest_versions_rejected(tmp_path, version):
    with pytest.raises(ValueError, match="Unsupported.*version"):
        validate(tmp_path, snapshot(tmp_path, version))


@pytest.mark.parametrize("source", ["bgt", "top10nl"])
def test_v3_allows_whole_objects_crossing_boundary(tmp_path, source):
    m = snapshot(tmp_path, "3")
    m["validation"]["extents"]["vectors"][source] = [[-100, -100, 100, 100]]
    validate(tmp_path, m)
    m["fixture_version"] = "2"
    m["clipping"]["vectors"] = "gdal"
    with pytest.raises(ValueError, match="exceeds the AOI"):
        validate(tmp_path, m)


@pytest.mark.parametrize(
    "category,source",
    [("vectors", "bag"), ("vectors", "cbs_buurtkaart"), ("pointclouds", "tile.laz")],
)
def test_v3_retains_other_containment_checks(tmp_path, category, source):
    m = snapshot(tmp_path, "3")
    extent = [-100, -100, 100, 100]
    m["validation"]["extents"][category][source] = (
        [extent] if category == "vectors" else extent
    )
    with pytest.raises(ValueError, match="exceeds the AOI"):
        validate(tmp_path, m)


@pytest.mark.parametrize(
    "extent",
    [
        [100, 100, 110, 110],
        [5, 5, 1, 1],
        [float("nan"), 1, 5, 5],
        [1, 1, float("inf"), 5],
    ],
)
def test_v3_rejects_invalid_or_disjoint_native_extents(tmp_path, extent):
    m = snapshot(tmp_path, "3")
    m["validation"]["extents"]["vectors"]["bgt"] = [extent]
    with pytest.raises(ValueError, match="extent"):
        validate(tmp_path, m)


def test_v3_native_extent_follows_declared_source_path(tmp_path):
    m = snapshot(tmp_path, "3")
    (tmp_path / "bgt").rename(tmp_path / "native.zip")
    m["sources"]["bgt"]["path"] = "native.zip"
    m["files"]["native.zip"] = m["files"].pop("bgt")
    m["validation"]["extents"]["vectors"]["native.zip"] = [[-100, -100, 100, 100]]
    del m["validation"]["extents"]["vectors"]["bgt"]
    validate(tmp_path, m)
    m["validation"]["extents"]["vectors"]["native.zip"] = []
    with pytest.raises(ValueError, match="Missing dataset extent"):
        validate(tmp_path, m)


def test_v3_rejects_legacy_processing_metadata(tmp_path):
    m = snapshot(tmp_path, "3")
    m["clipping"]["vectors"] = "gdal"
    with pytest.raises(ValueError, match="unsupported clipping"):
        validate(tmp_path, m)


@pytest.mark.parametrize("source", ["bgt", "top10nl"])
def test_v3_checks_combined_dataset_extent(tmp_path, source):
    m = snapshot(tmp_path, "3")
    extents: list[list[float]] = [[-100, -100, -50, -50], [50, 50, 100, 100]]
    m["validation"]["extents"]["vectors"][source] = extents
    validate(tmp_path, m)
    extents.append([float("nan"), 1, 2, 3])
    with pytest.raises(ValueError, match="Invalid computed extent"):
        validate(tmp_path, m)
