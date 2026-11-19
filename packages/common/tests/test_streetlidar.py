import json
import os
from dataclasses import asdict

import pytest
from bag3d.common.streetlidar import (
    CRS,
    discover,
    empty_catalog,
    ensure_tables,
    fixture_catalog,
    publish,
    read_catalog,
    selected_locations,
)
from bag3d.common.types import PostgresTableIdentifier


def source(tmp_path):
    root = tmp_path / "live"
    root.mkdir()
    manifest = {
        "version": 1,
        "crs": CRS,
        "datasets": [
            {"id": "amsterdam/2025", "path": "amsterdam/2025"},
            {"id": "other/2026", "path": "other/2026"},
        ],
    }
    (root / "manifest.json").write_text(json.dumps(manifest))
    for dataset in manifest["datasets"]:
        path = root / dataset["path"] / "nested" / "same-name.LAZ"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"sample")
    return root, manifest


def inspect(path):
    return (0.0, 0.0, 100.0, 100.0), 200, {"crs": CRS}


def test_discovery_changes_and_manifest_authority(tmp_path):
    root, manifest = source(tmp_path)
    ignored = root / "unlisted"
    ignored.mkdir()
    (ignored / "tile.laz").write_bytes(b"ignored")
    catalog = discover(root, inspect)
    assert len(catalog.tiles) == 2
    assert catalog.datasets == {"amsterdam/2025": 1, "other/2026": 1}
    previous = {tile.location: tile for tile in catalog.tiles}
    calls = []

    def reader(path):
        calls.append(path)
        return inspect(path)

    assert discover(root, reader, previous).identity == catalog.identity
    assert calls == []
    discover(root, reader, previous, force=True)
    assert len(calls) == 2
    calls.clear()
    (root / catalog.tiles[0].location).write_bytes(b"changed")
    changed = discover(root, reader, previous)
    assert len(calls) == 1 and changed.identity != catalog.identity
    manifest["datasets"].pop()
    (root / "manifest.json").write_text(json.dumps(manifest))
    assert len(discover(root, inspect).tiles) == 1


@pytest.mark.parametrize(
    "failure",
    ["missing", "empty", "duplicate", "escape", "crs", "invalid", "broken_link"],
)
def test_discovery_rejects_invalid_inputs(tmp_path, failure):
    root, manifest = source(tmp_path)
    if failure == "missing":
        manifest["datasets"][0]["path"] = "missing"
    elif failure == "empty":
        for path in (root / "amsterdam").rglob("*.LAZ"):
            path.unlink()
    elif failure == "duplicate":
        manifest["datasets"].append({"id": "duplicate", "path": "amsterdam/2025"})
    elif failure == "escape":
        manifest["datasets"][0]["path"] = "../outside"
    elif failure == "crs":
        manifest["crs"] = "EPSG:28992"
    elif failure == "broken_link":
        (root / "amsterdam/2025/broken.laz").symlink_to("missing.laz")
    (root / "manifest.json").write_text(json.dumps(manifest))
    reader = (
        (lambda path: ((float("nan"), 0, 1, 1), 3, {}))
        if failure == "invalid"
        else inspect
    )
    with pytest.raises((ValueError, FileNotFoundError)):
        discover(root, reader)


def test_copy_race_fails_discovery(tmp_path):
    root, _ = source(tmp_path)

    def changing(path):
        path.write_bytes(b"changed during header read")
        return inspect(path)

    with pytest.raises(ValueError, match="changed during"):
        discover(root, changing)


def test_fixture_catalog_relocation_and_declared_files_only(tmp_path):
    root, _ = source(tmp_path)
    catalog = discover(root, inspect)
    target = tmp_path / "snapshot" / "streetlidar"
    target.mkdir(parents=True)
    for tile in catalog.tiles:
        path = target / tile.location
        path.parent.mkdir(parents=True)
        path.write_bytes((root / tile.location).read_bytes())
    payload = {
        "version": 1,
        "crs": CRS,
        "manifest": catalog.manifest,
        "source_fingerprint": catalog.identity,
        "tiles": [asdict(t) for t in catalog.tiles],
    }
    (target / "catalog.json").write_text(json.dumps(payload))
    declared = {
        "streetlidar/catalog.json",
        *("streetlidar/" + t.location for t in catalog.tiles),
    }
    (target / "undeclared.laz").write_bytes(b"ignore")
    verified = set()

    def verify(name):
        if name not in declared:
            raise ValueError("undeclared file")
        verified.add(name)

    loaded = fixture_catalog(target.parent, "streetlidar/catalog.json", verify)
    assert len(loaded.tiles) == 2
    assert verified == declared
    assert loaded.source_fingerprint == catalog.identity
    fixture_root = loaded.root
    assert fixture_root is not None
    assert all(t.path(fixture_root).is_file() for t in loaded.tiles)
    declared.pop()
    with pytest.raises(ValueError, match="undeclared"):
        fixture_catalog(target.parent, "streetlidar/catalog.json", verify)


@pytest.fixture
def postgis():
    from psycopg import connect

    dsn = os.getenv("BAG3D_STREETLIDAR_TEST_DSN")
    if not dsn:
        pytest.skip("Disposable streetlidar PostGIS DSN not configured")
    with connect(dsn) as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        connection.execute("DROP SCHEMA IF EXISTS streetlidar CASCADE")
    try:
        yield dsn
    finally:
        with connect(dsn) as connection:
            connection.execute("DROP SCHEMA IF EXISTS streetlidar CASCADE")


def test_transactional_refresh_and_consistent_reader(tmp_path, postgis):
    from psycopg import connect

    root, manifest = source(tmp_path)
    catalog = discover(root, inspect)
    with connect(postgis) as connection:
        ensure_tables(connection)
        assert publish(connection, catalog)["added"] == 2
    with connect(postgis) as reader:
        reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        assert read_catalog(reader).identity == catalog.identity
        manifest["datasets"].pop()
        (root / "manifest.json").write_text(json.dumps(manifest))
        newer = discover(root, inspect)
        with connect(postgis) as writer:
            ensure_tables(writer)
            stats = publish(writer, newer)
            assert stats["removed"] == 1 and stats["changed"] == stats["added"] == 0
        assert read_catalog(reader).identity == catalog.identity
    with connect(postgis) as connection:
        ensure_tables(connection)
        assert read_catalog(connection).identity == newer.identity
        stats = publish(connection, newer)
        assert stats["changed"] == stats["added"] == stats["removed"] == 0
    with pytest.raises(RuntimeError):
        with connect(postgis) as connection:
            ensure_tables(connection)
            publish(connection, empty_catalog())
            raise RuntimeError("cancel refresh")
    with connect(postgis) as connection:
        assert read_catalog(connection).identity == newer.identity
        connection.execute(
            "CREATE TEMP TABLE polygons AS SELECT ST_MakeEnvelope(105,0,110,10,28992) geom"
        )
        assert not selected_locations(
            connection, PostgresTableIdentifier("pg_temp", "polygons"), 0
        )
        assert selected_locations(
            connection, PostgresTableIdentifier("pg_temp", "polygons"), 6
        )
        ensure_tables(connection)
        publish(connection, empty_catalog())
        assert read_catalog(connection).tiles == []
