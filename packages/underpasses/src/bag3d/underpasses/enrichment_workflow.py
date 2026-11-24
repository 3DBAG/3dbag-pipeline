"""Persistence adapters for the standalone underpass tools."""

import json
from collections import Counter
from hashlib import sha256
from pathlib import Path

from bag3d.common.streetlidar import read_catalog, selected_locations
from psycopg import connect, sql
from psycopg.types.json import Jsonb
from shapely import from_wkb


def terrain_inputs(path: Path):
    """Read the producer's manifest; never guess a content-addressed build path."""
    payload = path.read_bytes()
    manifest = json.loads(payload)
    if manifest.get("version") != 1 or manifest.get("height_reference") != "NAP":
        raise ValueError("Expected a version-1 terrain/ahn_filled manifest in NAP")
    files = manifest["files"]
    if not files:
        raise ValueError("Filled AHN manifest contains no rasters")
    for record in files:
        raster = Path(record["path"])
        if (
            not raster.is_absolute()
            or not raster.is_file()
            or raster.stat().st_size != record["bytes"]
        ):
            raise ValueError(f"Missing or changed terrain raster: {raster}")
    return tuple(record["path"] for record in files), sha256(payload).hexdigest()


def run_heights(
    dsn,
    source,
    target,
    terrain_manifest,
    settings,
    streetlidar_index=None,
    *,
    streetlidar_root: Path | None = None,
):
    from underpass_tools.heights import TileRecord, UnderpassRecord, estimate_heights
    from underpass_tools.lidar import result_metadata

    rasters, identity = terrain_inputs(Path(terrain_manifest))
    counts = Counter()
    lidar_identity = None
    dataset_ids = []
    with connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        tiles = []
        if streetlidar_index is not None:
            if streetlidar_root is None:
                raise ValueError(
                    "A streetlidar store is required with the managed index"
                )
            catalog = read_catalog(connection, streetlidar_index)
            streetlidar_root = catalog.validate_root(streetlidar_root)
            lidar_identity = catalog.identity
            locations = selected_locations(
                connection, source, settings.polygon_buffer, streetlidar_index
            )
            selected = [tile for tile in catalog.tiles if tile.location in locations]
            dataset_ids = sorted({tile.dataset_id for tile in selected})
            for tile in selected:
                from shapely.geometry import box

                tiles.append(TileRecord(tile.path(streetlidar_root), box(*tile.bounds)))
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target.id))
        cursor.execute(
            sql.SQL("CREATE TABLE {} AS SELECT * FROM {}").format(target.id, source.id)
        )
        cursor.execute(
            sql.SQL("""ALTER TABLE {} ADD PRIMARY KEY (identificatie, underpass_id),
            ADD COLUMN underpass_terrain_elevation double precision,
            ADD COLUMN underpass_candidate_elevations double precision,
            ADD COLUMN underpass_confidence double precision,
            ADD COLUMN underpass_source text,
            ADD COLUMN underpass_status text,
            ADD COLUMN underpass_metadata jsonb,
            ADD COLUMN underpass_updated_at timestamptz""").format(target.id)
        )
        cursor.execute(
            sql.SQL("CREATE INDEX ON {} USING gist (geom)").format(target.id)
        )
        cursor.execute(
            sql.SQL(
                "SELECT identificatie, underpass_id, ST_AsBinary(geom) FROM {} ORDER BY underpass_id"
            ).format(source.id)
        )
        records = [
            UnderpassRecord(identifier, uid, from_wkb(bytes(geometry)))
            for identifier, uid, geometry in cursor
        ]
        update = sql.SQL("""UPDATE {} SET underpass_terrain_elevation=%s,
            underpass_candidate_elevations=%s, underpass_confidence=%s,
            underpass_source=%s, underpass_status=%s, underpass_metadata=%s,
            underpass_updated_at=now() WHERE identificatie=%s AND underpass_id=%s""").format(
            target.id
        )
        seen = set()
        for results in estimate_heights(records, rasters, settings, tiles=tiles):
            rows = []
            for result in results:
                key = (result.identificatie, result.underpass_id)
                if key in seen:
                    raise ValueError(f"Duplicate height result: {key}")
                seen.add(key)
                counts[result.status] += 1
                metadata = {
                    **result_metadata(result),
                    "terrain_manifest_sha256": identity,
                    "streetlidar_catalog_fingerprint": lidar_identity,
                }
                rows.append(
                    (
                        result.terrain_elevation,
                        result.candidate_elevation,
                        result.confidence,
                        "streetlidar"
                        if result.candidate_elevation is not None
                        else "heuristic",
                        result.status,
                        Jsonb(metadata),
                        *key,
                    )
                )
            cursor.executemany(update, rows)
        if seen != {(r.identificatie, r.underpass_id) for r in records}:
            raise ValueError(
                "Height estimation did not return exactly one result per polygon"
            )
    return {
        "processed": len(seen),
        "statuses": dict(counts),
        "terrain_manifest_sha256": identity,
        "streetlidar_catalog_fingerprint": lidar_identity,
        "streetlidar_datasets": dataset_ids,
    }
