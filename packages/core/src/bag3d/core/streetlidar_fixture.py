"""Portable AOI streetlidar snapshots derived from the published PostGIS catalog."""

import json
import tempfile
from dataclasses import asdict
from pathlib import Path
from shlex import quote

from bag3d.common.resources.executables import LASToolsResource, PDALResource
from bag3d.common.streetlidar import CRS, Catalog, Tile, fingerprint, read_catalog
from bag3d.common.types import PostgresTableIdentifier
from psycopg import connect, sql

from bag3d.core.assets.streetlidar import inspect_tile


def crop_catalog(
    dsn: str,
    table: PostgresTableIdentifier,
    target: Path,
    bounds: tuple[float, float, float, float],
    lastools: LASToolsResource,
    pdal: PDALResource,
    *,
    streetlidar_root: Path,
) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    with connect(dsn) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        catalog = read_catalog(connection, table)
        streetlidar_root = catalog.validate_root(streetlidar_root)
        locations = {
            row[0]
            for row in connection.execute(
                sql.SQL(
                    "SELECT location FROM {} WHERE boundary && ST_MakeEnvelope(%s,%s,%s,%s,28992)"
                ).format(table.id),
                bounds,
            )
        }
    with tempfile.TemporaryDirectory(dir=target.parent) as temporary:
        working = Path(temporary) / "streetlidar"
        working.mkdir()
        tiles = []
        for tile in catalog.tiles:
            if tile.location not in locations:
                continue
            source = tile.path(streetlidar_root)
            destination = working / tile.location
            destination.parent.mkdir(parents=True, exist_ok=True)
            minx, miny, maxx, maxy = bounds
            result = lastools.runner.run(
                "{exe} -i {source} -keep_xy {minx} {miny} {maxx} {maxy} -o {output}",
                exe_name="las2las",
                kwargs={
                    "source": quote(str(source)),
                    "output": quote(str(destination)),
                    "minx": minx,
                    "miny": miny,
                    "maxx": maxx,
                    "maxy": maxy,
                },
            )
            if not result.success or not destination.is_file():
                raise RuntimeError(
                    f"Could not crop streetlidar tile {source}: {result.stderr}"
                )
            extent, count, metadata = inspect_tile(destination, pdal)
            if count == 0:
                destination.unlink()
                continue
            if not (
                minx <= extent[0] <= extent[2] <= maxx
                and miny <= extent[1] <= extent[3] <= maxy
            ):
                raise ValueError(
                    f"Streetlidar crop exceeds fixture bounds: {tile.location}"
                )
            stat = destination.stat()
            cropped = Tile(
                tile.dataset_id,
                tile.location,
                stat.st_size,
                stat.st_mtime_ns,
                extent,
                count,
                metadata,
            )
            cropped.validate()
            tiles.append(cropped)
        dataset_ids = {tile.dataset_id for tile in tiles}
        manifest = {
            **catalog.manifest,
            "datasets": [
                dataset
                for dataset in catalog.manifest["datasets"]
                if dataset["id"] in dataset_ids
            ],
        }
        output = Catalog(
            working, manifest, fingerprint(manifest), tiles, catalog.identity
        )
        payload = {
            "version": 1,
            "crs": CRS,
            "manifest": manifest,
            "source_fingerprint": output.source_fingerprint,
            "tiles": [asdict(tile) for tile in tiles],
        }
        (working / "catalog.json").write_text(
            json.dumps(payload, sort_keys=True, indent=2)
        )
        # Validate every source again before publishing the generated subtree.
        for tile in catalog.tiles:
            if tile.location in locations:
                tile.path(streetlidar_root)
        backup = Path(temporary) / "previous"
        if target.exists():
            target.rename(backup)
        try:
            working.rename(target)
        except BaseException:
            if backup.exists():
                backup.rename(target)
            raise
    return target / "catalog.json"
