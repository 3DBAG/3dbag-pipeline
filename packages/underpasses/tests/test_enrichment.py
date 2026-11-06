import json
import os
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from bag3d.common.types import PostgresTableIdentifier

from bag3d.underpasses.enrichment_workflow import terrain_inputs


def test_manifest_handoff_and_missing_file(tmp_path):
    raster = tmp_path / "terrain.tif"
    raster.write_bytes(b"sample")
    manifest = tmp_path / "manifest.json"
    data = {
        "version": 1,
        "height_reference": "NAP",
        "files": [
            {"path": str(raster), "bytes": 6, "sha256": sha256(b"sample").hexdigest()}
        ],
    }
    manifest.write_text(json.dumps(data))
    paths, identity = terrain_inputs(manifest)
    assert paths == (str(raster),)
    assert identity == sha256(manifest.read_bytes()).hexdigest()
    data["height_reference"] = "ellipsoidal"
    manifest.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="NAP"):
        terrain_inputs(manifest)
    data["height_reference"] = "NAP"
    manifest.write_text(json.dumps(data))
    raster.unlink()
    with pytest.raises(ValueError, match="Missing or changed"):
        terrain_inputs(manifest)


def test_pipeline_order_and_terrain_job():
    manifest = json.loads(
        (Path(__file__).resolve().parents[3] / "3dbag-manifest.json").read_text()
    )
    stages = [stage["name"] for stage in manifest["pipeline"]]
    assert (
        stages.index("terrain_ahn")
        < stages.index("underpasses")
        < stages.index("reconstruct")
        < stages.index("party_walls")
    )


@pytest.fixture
def enrichment_tables():
    from psycopg import connect, sql

    dsn = os.getenv("BAG3D_UNDERPASSES_TEST_DSN")
    if not dsn:
        pytest.skip("Disposable PostGIS DSN not configured")
    schema = "test_enrichment_" + uuid4().hex
    with connect(dsn) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        db.execute(
            sql.SQL(
                "CREATE TABLE {}.polygons (identificatie text, underpass_id integer, geom geometry(Polygon,28992))"
            ).format(sql.Identifier(schema))
        )
        db.execute(
            sql.SQL(
                "INSERT INTO {}.polygons VALUES ('B',1,ST_MakeEnvelope(3,-1,7,5,28992))"
            ).format(sql.Identifier(schema))
        )
    try:
        yield (
            dsn,
            PostgresTableIdentifier(schema, "polygons"),
            PostgresTableIdentifier(schema, "heights"),
        )
    finally:
        with connect(dsn) as db:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def filled_manifest(tmp_path):
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    path = tmp_path / "terrain.tif"
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="float32",
        crs="EPSG:28992",
        transform=from_origin(-10, 30, 5, 5),
        nodata=-9999,
    ) as ds:
        ds.write(np.zeros((8, 8), dtype="float32"), 1)
        ds.update_tags(HEIGHT_REFERENCE="NAP")
    manifest = tmp_path / "terrain_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "version": 1,
                "height_reference": "NAP",
                "files": [
                    {
                        "path": str(path),
                        "bytes": path.stat().st_size,
                        "sha256": sha256(path.read_bytes()).hexdigest(),
                    }
                ],
            }
        )
    )
    return manifest


@pytest.mark.integration
def test_height_rebuild_and_rollback(tmp_path, enrichment_tables, monkeypatch):
    import underpass_tools.heights as tools
    from psycopg import connect, sql
    from underpass_tools.heights import HeightSettings

    from bag3d.underpasses.enrichment_workflow import run_heights

    dsn, source, target = enrichment_tables
    manifest = filled_manifest(tmp_path)
    stats = run_heights(dsn, source, target, manifest, HeightSettings())
    assert stats["processed"] == 1
    with connect(dsn) as db:
        assert db.execute(
            sql.SQL("SELECT underpass_source,underpass_status FROM {}").format(
                target.id
            )
        ).fetchone() == ("heuristic", "no_laz_tiles")

    def broken(*args, **kwargs):
        raise RuntimeError("height worker crashed")

    monkeypatch.setattr(tools, "estimate_heights", broken)
    with pytest.raises(RuntimeError, match="crashed"):
        run_heights(dsn, source, target, manifest, HeightSettings())
    with connect(dsn) as db:
        assert db.execute(
            sql.SQL("SELECT count(*) FROM {}").format(target.id)
        ).fetchone() == (1,)
