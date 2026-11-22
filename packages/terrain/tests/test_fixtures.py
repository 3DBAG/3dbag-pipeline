"""Exercise relocated raw terrain fixtures through the normal AHN job."""

import importlib
import json
from pathlib import Path

import numpy as np
import pytest
import rasterio
from dagster import DagsterInstance
from rasterio.transform import from_origin
from rasterio.warp import transform_bounds

from bag3d.common.terrain import sha256_file, terrain_snapshot
from bag3d.terrain import processing
from bag3d.terrain.components.terrain_component import TerrainComponent
from bag3d.terrain.state import file_record, read_manifest, save_manifest


def publish_snapshot(root):
    files = {
        str(path.relative_to(root)): {
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in (root / "terrain").rglob("*")
        if path.is_file()
    }
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "fixture_version": "3",
                "sources": {"terrain": {"path": "terrain/catalog.json"}},
                "files": files,
            }
        )
    )


@pytest.fixture
def raw_snapshot(tmp_path, write_raster, monkeypatch):
    monkeypatch.syspath_prepend(
        str(Path(__file__).resolve().parents[2] / "core" / "src")
    )
    crop_terrain = importlib.import_module("bag3d.core.terrain_fixture").crop_terrain

    live = tmp_path / "live"
    values = np.full((400, 300), 7.25, dtype="float32")
    values[:, 260:] = -9999
    left = write_raster(
        live / "left.tif", values, transform=from_origin(120900, 486900, 5, 5)
    )
    right = write_raster(
        live / "right.tif",
        np.full((400, 300), 7.25),
        transform=from_origin(122400, 486900, 5, 5),
    )
    # Overlap has distinct heights to exercise source order as well as adjacency.
    overlap = write_raster(
        live / "overlap.tif",
        np.full((50, 50), 8.5),
        transform=from_origin(122500, 486000, 5, 5),
    )
    sources = [
        file_record(path, url=f"https://fixture/{path.name}")
        for path in (left, right, overlap)
    ]
    bounds = list(transform_bounds(28992, 4326, 120900, 484900, 123900, 486900))
    manifest = save_manifest(live / "ahn.json", sources, bounds=bounds)
    root = tmp_path / "snapshot"
    crop_terrain(manifest, live, root / "terrain", (122000, 485750, 123000, 486000))
    publish_snapshot(root)
    return root, manifest


def test_snapshot_runs_offline_after_relocation(tmp_path, raw_snapshot, monkeypatch):
    root, live_manifest = raw_snapshot
    reference = read_manifest(
        processing.fill_ahn(
            tmp_path / "reference",
            processing.normalize_ahn(tmp_path / "reference", live_manifest, 1),
            500,
            1,
        )
    )
    relocated = tmp_path / "relocated"
    root.rename(relocated)
    before = {
        str(p.relative_to(relocated)): p.read_bytes()
        for p in relocated.rglob("*")
        if p.is_file()
    }
    for path in relocated.rglob("*"):
        path.chmod(0o555 if path.is_dir() else 0o444)
    relocated.chmod(0o555)
    monkeypatch.setenv("BAG3D_INPUT_MODE", "integration_data")
    monkeypatch.setenv("BAG3D_INTEGRATION_DATA_DIR", str(relocated))
    monkeypatch.setenv("BAG3D_TERRAIN_DIR", str(tmp_path / "outputs"))
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *args: pytest.fail("Fixture run accessed network"),
    )
    definitions = TerrainComponent().build_defs()
    assert definitions.jobs is not None
    assert [job.name for job in definitions.jobs] == ["terrain_ahn"]
    config = {"resources": {"terrain": {"config": {"workers": 1}}}}
    with DagsterInstance.ephemeral(tempdir=str(tmp_path / "instance")) as instance:
        run = definitions.resolve_job_def("terrain_ahn").execute_in_process(
            instance=instance, run_config=config
        )
    assert run.success
    assert run.output_for_node("ahn_catalog") == run.output_for_node("ahn_download")
    for stage in ("ahn_download", "ahn_rasters"):
        with pytest.raises(ValueError, match="current bounds and radius"):
            processing.validate_selection(run.output_for_node(stage), None, 600)
    filled_path = run.output_for_node("ahn_filled")
    filled = read_manifest(filled_path)
    assert filled["height_reference"] == "NAP"
    # Compare measurements and filled values throughout the integration AOI.
    for actual, expected in zip(filled["files"], reference["files"], strict=True):
        with (
            rasterio.open(actual["path"]) as cropped,
            rasterio.open(expected["path"]) as full,
        ):
            for x, y in [(122100, 485800), (122350, 485850), (122600, 485900)]:
                if (
                    cropped.bounds.left <= x < cropped.bounds.right
                    and cropped.bounds.bottom < y <= cropped.bounds.top
                ):
                    np.testing.assert_allclose(
                        next(cropped.sample([(x, y)])),
                        next(full.sample([(x, y)])),
                        atol=0.0011,
                    )
    assert all(
        Path(record["path"]).is_relative_to(tmp_path / "outputs")
        for record in filled["files"]
    )
    assert before == {
        str(p.relative_to(relocated)): p.read_bytes()
        for p in relocated.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("failure", ["checksum", "escape", "missing"])
def test_snapshot_rejects_bad_inputs(raw_snapshot, failure):
    root, _ = raw_snapshot
    if failure == "missing":
        payload = json.loads((root / "manifest.json").read_text())
        payload["sources"].pop("terrain")
        (root / "manifest.json").write_text(json.dumps(payload))
    elif failure == "checksum":
        raster = next((root / "terrain").glob("*.tif"))
        raster.write_bytes(b"corrupt")
    else:
        catalog_path = root / "terrain/catalog.json"
        catalog = json.loads(catalog_path.read_text())
        catalog["files"][0]["path"] = "../escape.tif"
        catalog_path.write_text(json.dumps(catalog))
        publish_snapshot(root)
    with pytest.raises(ValueError):
        terrain_snapshot(root)


def test_failed_crop_preserves_snapshot(raw_snapshot, monkeypatch):
    crop_terrain = importlib.import_module("bag3d.core.terrain_fixture").crop_terrain

    root, manifest = raw_snapshot
    before = (root / "terrain/catalog.json").read_bytes()
    with pytest.raises(ValueError, match="does not cover"):
        crop_terrain(
            manifest,
            manifest.parent,
            root / "terrain",
            (135000, 450000, 136000, 451000),
        )
    assert (root / "terrain/catalog.json").read_bytes() == before


@pytest.mark.parametrize(
    "settings, message",
    [
        ({"interpolation_radius_m": 600}, "radius exceeds"),
        ({"bounds": [4.0, 51.0, 4.01, 51.01]}, "bounds exceed"),
    ],
)
def test_fixture_rejects_settings_outside_snapshot(
    raw_snapshot, tmp_path, monkeypatch, settings, message
):
    root, _ = raw_snapshot
    monkeypatch.setenv("BAG3D_INPUT_MODE", "integration_data")
    monkeypatch.setenv("BAG3D_INTEGRATION_DATA_DIR", str(root))
    monkeypatch.setenv("BAG3D_TERRAIN_DIR", str(tmp_path / "outputs"))
    definitions = TerrainComponent().build_defs()
    with pytest.raises(Exception, match=message):
        definitions.resolve_job_def("terrain_ahn").execute_in_process(
            run_config={"resources": {"terrain": {"config": settings}}},
        )


def test_failed_crop_does_not_replace_previous_rasters(raw_snapshot):
    crop_terrain = importlib.import_module("bag3d.core.terrain_fixture").crop_terrain

    root, manifest = raw_snapshot
    before = {
        str(p.relative_to(root)): p.read_bytes()
        for p in (root / "terrain").rglob("*")
        if p.is_file()
    }
    source = Path(read_manifest(manifest)["files"][1]["path"])
    # The first crop completes before this changed source fails validation.
    with rasterio.open(source, "r+") as raster:
        values = raster.read(1)
        values[0, 0] = 9
        raster.write(values, 1)
    with pytest.raises(ValueError, match="changed"):
        crop_terrain(
            manifest,
            manifest.parent,
            root / "terrain",
            (122000, 485750, 123000, 486000),
        )
    assert before == {
        str(p.relative_to(root)): p.read_bytes()
        for p in (root / "terrain").rglob("*")
        if p.is_file()
    }


def test_rejects_catalog_without_required_neighbors(raw_snapshot):
    root, _ = raw_snapshot
    path = root / "terrain/catalog.json"
    catalog = json.loads(path.read_text())
    catalog["selection_bounds_rd"] = catalog["bounds_rd"]
    path.write_text(json.dumps(catalog))
    publish_snapshot(root)
    with pytest.raises(ValueError, match="insufficient interpolation neighbors"):
        terrain_snapshot(root)


def test_missing_tile_inside_outer_extent_preserves_snapshot(
    raw_snapshot, write_raster
):
    root, manifest_path = raw_snapshot
    crop_terrain = importlib.import_module("bag3d.core.terrain_fixture").crop_terrain
    before = (root / "terrain/catalog.json").read_bytes()
    manifest = read_manifest(manifest_path)
    left = write_raster(
        manifest_path.parent / "short-left.tif",
        np.full((400, 260), 7.25),
        transform=from_origin(120900, 486900, 5, 5),
    )
    # Both outer corners remain covered, but the 200 m strip before right.tif is absent.
    manifest["files"][0] = file_record(left, url="https://fixture/left.tif")
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="insufficient coverage"):
        crop_terrain(
            manifest_path,
            manifest_path.parent,
            root / "terrain",
            (122000, 485750, 123000, 486000),
        )
    assert (root / "terrain/catalog.json").read_bytes() == before


def test_snapshot_reader_rejects_gap_inside_outer_extent(raw_snapshot):
    root, _ = raw_snapshot
    path = root / "terrain/catalog.json"
    catalog = json.loads(path.read_text())
    catalog["files"][0]["bounds_rd"][2] = 122200
    path.write_text(json.dumps(catalog))
    publish_snapshot(root)
    with pytest.raises(ValueError, match="insufficient coverage"):
        terrain_snapshot(root)
