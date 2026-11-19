"""PostGIS streetlidar fixture and ceiling-estimation integration."""

import json
import os
import shutil
from pathlib import Path

import pytest
from bag3d.common.streetlidar import (
    CRS,
    TILE_INDEX,
    discover,
    ensure_tables,
    fixture_catalog,
    publish,
)


@pytest.mark.integration
def test_postgis_fixture_height_round_trip(tmp_path, monkeypatch):
    dsn = os.getenv("BAG3D_STREETLIDAR_TEST_DSN")
    pdal_exe = os.getenv("BAG3D_TEST_PDAL") or shutil.which("pdal")
    las2las_exe = os.getenv("BAG3D_TEST_LAS2LAS") or shutil.which("las2las")
    if not dsn or not pdal_exe or not las2las_exe:
        pytest.skip("Disposable PostGIS DSN, PDAL and las2las required")
    # Core owns fixture generation; this integration also exercises its producer.
    core_src = Path(__file__).resolve().parents[2] / "core" / "src"
    if not core_src.is_dir():
        pytest.skip("Cross-package integration requires the repository checkout")
    monkeypatch.syspath_prepend(str(core_src))
    producer = pytest.importorskip("bag3d.core.assets.streetlidar")
    crop_catalog = pytest.importorskip("bag3d.core.streetlidar_fixture").crop_catalog
    inspect_tile = producer.inspect_tile
    tile_index = producer.tile_index

    from hashlib import sha256

    import laspy
    import numpy as np
    import rasterio
    from bag3d.common.resources.executables import LASToolsResource, PDALResource
    from bag3d.common.resources.database import DatabaseResource
    from bag3d.common.types import PostgresTableIdentifier
    from dagster import materialize
    from psycopg import connect
    from psycopg.conninfo import conninfo_to_dict
    from pyproj import CRS as ProjCRS
    from rasterio.transform import from_origin
    from underpass_tools.heights import HeightSettings

    from bag3d.underpasses.enrichment_workflow import run_heights

    root = tmp_path / "live"
    dataset = root / "amsterdam/2025"
    dataset.mkdir(parents=True)
    manifest = {
        "version": 1,
        "crs": CRS,
        "datasets": [{"id": "amsterdam/2025", "path": "amsterdam/2025"}],
    }
    (root / "manifest.json").write_text(json.dumps(manifest))
    header = laspy.LasHeader(point_format=6, version="1.4")
    header.add_crs(ProjCRS.from_epsg(7415))
    cloud = laspy.LasData(header)
    rng = np.random.default_rng(123)
    cloud.x = rng.uniform(122010, 122040, 4000)
    cloud.y = rng.uniform(485760, 485790, 4000)
    cloud.z = np.full(4000, 3.3)
    cloud.write(dataset / "tile.laz")
    pdal = PDALResource(exe_pdal=pdal_exe)
    lastools = LASToolsResource(
        exe_las2las=las2las_exe, exe_lasinfo=las2las_exe, exe_lasindex=las2las_exe
    )
    catalog = discover(root, lambda path: inspect_tile(path, pdal))
    with connect(dsn) as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    db_config = {
        name: str(value)
        for name, value in conninfo_to_dict(dsn).items()
        if value is not None
    }
    database = DatabaseResource(
        host=db_config["host"],
        port=int(db_config["port"]),
        user=db_config["user"],
        password=db_config.get("password"),
        dbname=db_config["dbname"],
    )
    result = materialize(
        [tile_index],
        resources={"computation_db": database, "pdal": pdal},
        run_config={"ops": {tile_index.op.name: {"config": {"root": str(root)}}}},
    )
    assert result.success
    assert result.output_for_node(tile_index.op.name) == TILE_INDEX
    target = tmp_path / "snapshot/streetlidar"
    target.mkdir(parents=True)
    (target / "stale.laz").write_bytes(b"old crop")
    crop_catalog(
        dsn, TILE_INDEX, target, (122000, 485750, 122050, 485800), lastools, pdal
    )
    assert not (target / "stale.laz").exists()
    relocated = tmp_path / "relocated"
    shutil.copytree(target.parent, relocated)
    files = {
        str(p.relative_to(relocated)): sha256(p.read_bytes()).hexdigest()
        for p in relocated.rglob("*")
        if p.is_file()
    }

    def verify(relative):
        assert relative in files
        assert (
            sha256((relocated / relative).read_bytes()).hexdigest() == files[relative]
        )

    loaded = fixture_catalog(relocated, "streetlidar/catalog.json", verify)
    assert loaded.source_fingerprint == catalog.identity
    root.rename(tmp_path / "live-unavailable")
    with connect(dsn) as connection:
        ensure_tables(connection)
        publish(connection, loaded)
        connection.execute("CREATE SCHEMA IF NOT EXISTS streetlidar_test")
        connection.execute("DROP TABLE IF EXISTS streetlidar_test.polygons")
        connection.execute(
            "CREATE TABLE streetlidar_test.polygons AS SELECT 'B'::text identificatie, 1::integer underpass_id, ST_MakeEnvelope(122000,485750,122050,485800,28992) geom"
        )
    raster = tmp_path / "terrain.tif"
    with rasterio.open(
        raster,
        "w",
        driver="GTiff",
        width=10,
        height=10,
        count=1,
        dtype="float32",
        crs="EPSG:28992",
        transform=from_origin(122000, 485800, 5, 5),
        nodata=-9999,
    ) as ds:
        ds.write(np.zeros((10, 10), dtype="float32"), 1)
        ds.update_tags(HEIGHT_REFERENCE="NAP")
    terrain_manifest = tmp_path / "terrain.json"
    terrain_manifest.write_text(
        json.dumps(
            {
                "version": 1,
                "height_reference": "NAP",
                "files": [{"path": str(raster), "bytes": raster.stat().st_size}],
            }
        )
    )
    source = PostgresTableIdentifier("streetlidar_test", "polygons")
    result_table = PostgresTableIdentifier("streetlidar_test", "heights")
    stats = run_heights(
        dsn,
        source,
        result_table,
        terrain_manifest,
        HeightSettings(batch_workers=1, pip_workers=1),
        TILE_INDEX,
    )
    assert stats["statuses"] == {"success": 1}
    assert stats["streetlidar_catalog_fingerprint"] == loaded.identity
    with connect(dsn) as connection:
        row = connection.execute(
            "SELECT underpass_candidate_elevations, underpass_metadata FROM streetlidar_test.heights"
        ).fetchone()
        assert row is not None
        height, provenance = row
        assert height == pytest.approx(3.3, abs=0.15)
        assert provenance["streetlidar_catalog_fingerprint"] == loaded.identity
    (relocated / "streetlidar" / loaded.tiles[0].location).unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        run_heights(
            dsn, source, result_table, terrain_manifest, HeightSettings(), TILE_INDEX
        )
    with connect(dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM streetlidar_test.heights"
        ).fetchone() == (1,)
        connection.execute("DROP SCHEMA streetlidar_test CASCADE")
        connection.execute("DROP SCHEMA streetlidar CASCADE")
