"""Discover manually supplied streetlidar datasets at materialization time."""

import json
from pathlib import Path
from shlex import quote

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import PDALResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.streetlidar import (
    TILE_INDEX,
    discover,
    ensure_tables,
    inventory,
    publish,
    read_catalog,
)
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import postgrestable_metadata
from dagster import Config, DataVersion, Output, asset
from psycopg import connect


class StreetLidarConfig(Config):
    force: bool = False


def inspect_tile(
    path: Path, pdal: PDALResource
) -> tuple[tuple[float, float, float, float], int, dict]:
    """Read LAS header metadata, without scanning the point payload."""
    from pyproj import CRS

    runner = pdal.runner
    if runner.with_docker:
        assert runner.container_mount_point is not None
        source = runner.container_mount_point / path.name
    else:
        source = path
    result = runner.run(
        "{exe} info --metadata {source}",
        exe_name="pdal",
        local_path=path,
        kwargs={"source": quote(str(source))},
    )
    if not result.success:
        raise RuntimeError(f"PDAL failed for {path}: {result.stderr or result.stdout}")
    metadata = json.loads(result.stdout)["metadata"]
    srs = metadata.get("srs") or {}
    wkt = (
        srs.get("compoundwkt")
        or srs.get("wkt")
        or metadata.get("comp_spatialreference")
        or metadata.get("spatialreference")
    )
    if wkt:
        declared = CRS.from_user_input(wkt)
        components = declared.sub_crs_list if declared.is_compound else [declared]
        horizontal = [part for part in components if part.is_projected]
        vertical = [part for part in components if part.is_vertical]
        if len(horizontal) != 1 or not horizontal[0].equals(CRS.from_epsg(28992)):
            raise ValueError(
                f"Streetlidar horizontal CRS conflicts with EPSG:7415: {path}"
            )
        if vertical and (
            len(vertical) != 1 or not vertical[0].equals(CRS.from_epsg(5709))
        ):
            raise ValueError(
                f"Streetlidar vertical CRS conflicts with EPSG:7415: {path}"
            )
    return (
        (
            float(metadata["minx"]),
            float(metadata["miny"]),
            float(metadata["maxx"]),
            float(metadata["maxy"]),
        ),
        int(metadata["count"]),
        metadata,
    )


@asset(key_prefix="streetlidar", group_name="streetlidar")
def tile_index(
    config: StreetLidarConfig,
    computation_db: DatabaseResource,
    pdal: PDALResource,
    streetlidar_store: FileStoreResource,
) -> Output[PostgresTableIdentifier]:
    """Ready streetlidar tiles from all directories in the root manifest."""
    root = Path(streetlidar_store.root_dir).resolve()
    with connect(computation_db.connection.dsn) as connection:
        ensure_tables(connection)
        previous = {}
        state = connection.execute(
            "SELECT root FROM streetlidar.catalog_state WHERE singleton"
        ).fetchone()
        if state and state[0] == str(root):
            previous = {tile.location: tile for tile in read_catalog(connection).tiles}
        catalog = discover(
            root, lambda path: inspect_tile(path, pdal), previous, config.force
        )
        stats = publish(connection, catalog)
        # Publication can take time: never commit an inventory that changed meanwhile.
        manifest, digest, files = inventory(root)
        if (
            manifest != catalog.manifest
            or digest != catalog.manifest_sha256
            or files
            != {
                tile.location: (tile.dataset_id, tile.size, tile.mtime_ns)
                for tile in catalog.tiles
            }
        ):
            raise ValueError(
                "Streetlidar source changed during publication; retry refresh"
            )
    return Output(
        TILE_INDEX,
        metadata={**postgrestable_metadata(computation_db, TILE_INDEX), **stats},
        data_version=DataVersion(catalog.identity),
    )
