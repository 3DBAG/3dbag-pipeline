"""Opt-in offline acceptance test with real PROJ grids and local GDAL."""

import os
from pathlib import Path
import shutil

from dagster import DagsterInstance
import mercantile
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from rasterio.warp import transform_bounds

from bag3d.terrain import downloads
from bag3d.terrain.code_location import defs
from bag3d.terrain.state import file_record, read_manifest, save_manifest
from bag3d.terrain.tiles import decode
from pmtiles.reader import MmapSource, all_tiles


@pytest.mark.integration
def test_full_workflow_and_offline_processing_rerun(
    tmp_path, write_raster, monkeypatch
):
    grid_dir = os.getenv("TERRAIN_TEST_GRID_DIR")
    if not grid_dir or not shutil.which("gdalwarp"):
        pytest.skip(
            "Set TERRAIN_TEST_GRID_DIR and put GDAL on PATH for offline acceptance"
        )
    values = np.full((256, 256), 2.0, dtype="float32")
    values[16:240, 16:240] = -9999
    raw = write_raster(tmp_path / "ahn.tif", values)
    with rasterio.open(raw) as raster:
        bounds = list(transform_bounds(28992, 4326, *raster.bounds))
    glo = write_raster(
        tmp_path / "glo.tif",
        np.zeros((256, 256)),
        crs=4326,
        transform=from_origin(4.2, 52.2, 0.001, 0.001),
    )
    catalog = save_manifest(tmp_path / "catalog.json", [])
    ahn = save_manifest(
        tmp_path / "ahn.json",
        [file_record(raw, url="https://fixture/ahn.tif")],
        bounds=bounds,
    )
    fallback = save_manifest(
        tmp_path / "glo.json",
        [file_record(glo, url="https://fixture/glo.tif")],
        bounds=bounds,
        cells=[{"lat": 52, "lon": 4, "url": "https://fixture/glo.tif"}],
    )
    grids = save_manifest(
        tmp_path / "grids.json",
        [
            file_record(Path(grid_dir) / name, name=name)
            for name in downloads.GRID_NAMES
        ],
    )
    monkeypatch.setattr(downloads, "ahn_catalog", lambda *a: catalog)
    monkeypatch.setattr(downloads, "download_ahn", lambda *a: ahn)
    monkeypatch.setattr(downloads, "download_glo30", lambda *a: fallback)
    monkeypatch.setattr(downloads, "download_grids", lambda *a: grids)
    config = {
        "resources": {
            "file_store": {"config": {"root_dir": str(tmp_path / "store")}},
            "terrain": {"config": {"workers": 1}},
        }
    }
    with DagsterInstance.ephemeral(tempdir=str(tmp_path / "instance")) as instance:
        full = defs.resolve_job_def("terrain").execute_in_process(
            instance=instance, run_config=config
        )
        assert full.success
        xyz_manifest = full.output_for_node("terrain_xyz")
        tiles_manifest = read_manifest(full.output_for_node("terrain_tiles"))
        assert tiles_manifest["fallback_pixels"] > 0
        # Captured using the original local Mapterhorn scripts, not this engine.
        with np.load(
            Path(__file__).parent / "data/mapterhorn-reference.npz"
        ) as reference:
            for record in tiles_manifest["files"]:
                with Path(record["path"]).open("rb") as stream:
                    for coordinates, data in all_tiles(MmapSource(stream)):
                        expected = reference["_".join(str(v) for v in coordinates)]
                        np.testing.assert_allclose(
                            decode(data), expected, rtol=0, atol=0.125
                        )
        outputs = read_manifest(xyz_manifest)
        tilejson_path = next(
            Path(r["path"])
            for r in outputs["files"]
            if r["path"].endswith("tilejson.json")
        )
        tilejson = read_manifest(tilejson_path)
        assert tilejson["minzoom"] == 0
        assert tilejson["maxzoom"] == 14
        assert tilejson["bounds"] == bounds
        expected_count = sum(
            sum(1 for _ in mercantile.tiles(*bounds, zooms=z)) for z in range(15)
        )
        assert outputs["xyz_tiles"] == expected_count
        before = {
            r["path"]: Path(r["path"]).stat().st_mtime_ns
            for r in outputs["files"]
            if r["path"].endswith(".webp")
        }

        def no_network(*args, **kwargs):
            raise AssertionError("Processing attempted network acquisition")

        for name in ("ahn_catalog", "download_ahn", "download_glo30", "download_grids"):
            monkeypatch.setattr(downloads, name, no_network)
        monkeypatch.setattr("requests.sessions.Session.request", no_network)
        config["resources"]["terrain"]["config"]["base_url"] = (
            "https://example.com/terrain"
        )
        rerun = defs.resolve_job_def("terrain_process").execute_in_process(
            instance=instance, run_config=config
        )
        assert rerun.success
        assert all(Path(p).stat().st_mtime_ns == mtime for p, mtime in before.items())
        assert read_manifest(tilejson_path)["tiles"] == [
            "https://example.com/terrain/{z}/{x}/{y}.webp"
        ]


@pytest.mark.integration
def test_ocean_zero_is_converted_before_encoding(tmp_path):
    from bag3d.terrain._mapterhorn import fallback
    from bag3d.terrain.state import digest_file

    grid_dir = os.getenv("TERRAIN_TEST_GRID_DIR")
    if not grid_dir:
        pytest.skip("Set TERRAIN_TEST_GRID_DIR for offline ocean height acceptance")
    grid = Path(grid_dir) / fallback.GRID
    fallback.init_worker(str(grid))
    output = tmp_path / "ocean.tif"
    fallback.convert_cell((53, 3, None, output, digest_file(grid)))
    with rasterio.open(output) as raster:
        value = raster.read(1, window=((100, 101), (100, 101)))[0, 0]
        assert 30 < value < 60
        assert raster.tags()["HEIGHT_REFERENCE"] == "ellipsoidal"
        assert raster.tags()["INPUT_HEIGHT_REFERENCE"] == "EGM2008 (EPSG:3855)"
