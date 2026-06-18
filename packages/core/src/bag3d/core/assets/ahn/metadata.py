import json
from datetime import datetime
from functools import partial

import pytz
from dagster import (
    asset,
    Output,
    Config,
    get_dagster_logger,
    AssetExecutionContext,
    AutomationCondition,
)
from bag3d.common.types import PostgresTableIdentifier
from psycopg.sql import Identifier, Literal, SQL
from psycopg.types.json import Jsonb, set_json_dumps
from pydantic import Field

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import PDALResource
from bag3d.common.utils.geodata import pdal_info
from bag3d.common.utils.database import create_schema, load_sql
from bag3d.core.assets.ahn.core import (
    partition_definition_ahn,
    partition_definition_ahn6_batches,
    tiles_in_batch,
)
from bag3d.core.assets.ahn.download import BatchLAZDownload

class MetadataConfig(Config):
    """Configuration for AHN metadata assets."""

    all: bool = Field(default=True, description="Run `pdal info` with `--all`.")
    force: bool = Field(
        default=True, description="Force the re-compute of the metadata."
    )
    verbose: bool = Field(default=False, description="Output stdout/stderr from pdal")


@asset(automation_condition=AutomationCondition.on_cron("0 0 9 * *"))
def metadata_table_ahn3(computation_db: DatabaseResource) -> PostgresTableIdentifier:
    """A metadata table for the AHN3, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(computation_db, ahn_version=3)


@asset(automation_condition=AutomationCondition.on_cron("0 0 9 * *"))
def metadata_table_ahn4(computation_db: DatabaseResource) -> PostgresTableIdentifier:
    """A metadata table for the AHN4, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(computation_db, ahn_version=4)


@asset(automation_condition=AutomationCondition.on_cron("0 0 9 * *"))
def metadata_table_ahn5(computation_db: DatabaseResource) -> PostgresTableIdentifier:
    """A metadata table for the AHN5, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(computation_db, ahn_version=5)


@asset(automation_condition=AutomationCondition.on_cron("0 0 9 * *"))
def metadata_table_ahn6(computation_db: DatabaseResource) -> PostgresTableIdentifier:
    """A metadata table for AHN6 COPC/LAZ point clouds."""
    return metadata_table_ahn(computation_db, ahn_version=6)


@asset(partitions_def=partition_definition_ahn, pool="ahn_metadata")
def metadata_ahn3(
    context: AssetExecutionContext,
    config: MetadataConfig,
    laz_files_ahn3,
    metadata_table_ahn3,
    tile_index_ahn,
    computation_db: DatabaseResource,
    pdal: PDALResource,
) -> Output[None]:
    """Metadata of the AHN3 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context.partition_key,
        config,
        laz_files_ahn3,
        metadata_table_ahn3,
        tile_index_ahn,
        computation_db,
        pdal,
    )


@asset(partitions_def=partition_definition_ahn, pool="ahn_metadata")
def metadata_ahn4(
    context: AssetExecutionContext,
    config: MetadataConfig,
    laz_files_ahn4,
    metadata_table_ahn4,
    tile_index_ahn,
    computation_db: DatabaseResource,
    pdal: PDALResource,
) -> Output[None]:
    """Metadata of the AHN4 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context.partition_key,
        config,
        laz_files_ahn4,
        metadata_table_ahn4,
        tile_index_ahn,
        computation_db,
        pdal,
    )


@asset(partitions_def=partition_definition_ahn, pool="ahn_metadata")
def metadata_ahn5(
    context: AssetExecutionContext,
    config: MetadataConfig,
    laz_files_ahn5,
    metadata_table_ahn5,
    tile_index_ahn,
    computation_db: DatabaseResource,
    pdal: PDALResource,
) -> Output[None]:
    """Metadata of the AHN5 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context.partition_key,
        config,
        laz_files_ahn5,
        metadata_table_ahn5,
        tile_index_ahn,
        computation_db,
        pdal,
    )


@asset(partitions_def=partition_definition_ahn6_batches, pool="ahn_metadata")
def metadata_ahn6(
    context: AssetExecutionContext,
    config: MetadataConfig,
    laz_files_ahn6: BatchLAZDownload,
    metadata_table_ahn6: PostgresTableIdentifier,
    tile_index_ahn6: dict,
    computation_db: DatabaseResource,
    pdal: PDALResource,
) -> Output[dict]:
    """Batched metadata extraction for AHN6 COPC/LAZ point clouds.

    Each partition processes a 10×10 km block. For every 1×1 km tile in the
    block that has a file on disk, PDAL info is extracted and loaded into the
    metadata table together with the tile boundary geometry.
    """
    batch_id = context.partition_key
    tiles = tiles_in_batch(batch_id)
    if not tiles:
        return Output({"batch": batch_id, "processed": 0, "total": 0})

    logger = get_dagster_logger()
    conn = computation_db.connection
    metadata_table = metadata_table_ahn6.id
    total = len(tiles)
    processed = 0
    failed = 0
    skipped = 0

    set_json_dumps(dumps=partial(json.dumps, ensure_ascii=False))
    insert_time = Literal(datetime.now(tz=pytz.timezone("Europe/Amsterdam")))

    for tile_id, laz_download in laz_files_ahn6.tiles.items():
        if not laz_download.success:
            failed += 1
            logger.warning(f"AHN6 tile {tile_id}: download was not successful")
            continue

        fpath = laz_download.path
        if not fpath.is_file():
            failed += 1
            logger.warning(f"AHN6 tile {tile_id}: file not found on disk — {fpath}")
            continue

        try:
            _, out_info = pdal_info(pdal.runner, file_path=fpath, with_all=config.all)
        except Exception:
            logger.warning(f"AHN6 tile {tile_id}: PDAL info failed for {fpath}")
            failed += 1
            continue

        boundary = tile_index_ahn6[tile_id]["geometry"]

        conn.send_query(
            SQL("DELETE FROM {table} WHERE tile_id = {tile_id}").format(
                table=metadata_table, tile_id=Literal(tile_id)
            )
        )
        conn.send_query(
            SQL("""
                INSERT INTO {table}(
                    tile_id, insert_time, pdal_info, boundary
                )
                VALUES (
                    {tile_id}, {insert_time}, {pdal_info},
                    ST_SetSRID(ST_GeomFromGeoJSON({boundary}), 28992)
                );
            """).format(
                table=metadata_table,
                tile_id=Literal(tile_id),
                insert_time=insert_time,
                pdal_info=Jsonb(out_info),
                boundary=Literal(json.dumps(boundary)),
            )
        )
        processed += 1

    logger.info(
        f"Batch {batch_id}: {processed} processed, "
        f"{failed} failed, {skipped} skipped of {total} tiles"
    )

    return Output(
        {"batch": batch_id, "processed": processed, "total": total},
        metadata={
            "batch": batch_id,
            "processed": processed,
            "failed": failed,
            "total": total,
        },
    )


@asset(deps=["metadata_ahn3"])
def metadata_ahn3_index(
    computation_db: DatabaseResource,
    metadata_table_ahn3: PostgresTableIdentifier,
):
    """Create indices on the AHN3 metadata table."""
    create_indices_metadata_table(computation_db, metadata_table_ahn3)
    return metadata_table_ahn3


@asset(deps=["metadata_ahn4"])
def metadata_ahn4_index(
    computation_db: DatabaseResource,
    metadata_table_ahn4: PostgresTableIdentifier,
):
    """Create indices on the AHN4 metadata table."""
    create_indices_metadata_table(computation_db, metadata_table_ahn4)
    return metadata_table_ahn4


@asset(deps=["metadata_ahn5"])
def metadata_ahn5_index(
    computation_db: DatabaseResource,
    metadata_table_ahn5: PostgresTableIdentifier,
):
    """Create indices on the AHN5 metadata table."""
    create_indices_metadata_table(computation_db, metadata_table_ahn5)
    return metadata_table_ahn5


@asset(deps=["metadata_ahn6"])
def metadata_ahn6_index(
    computation_db: DatabaseResource,
    metadata_table_ahn6: PostgresTableIdentifier,
):
    """Create indices on the AHN6 metadata table."""
    create_indices_metadata_table(computation_db, metadata_table_ahn6)
    return metadata_table_ahn6


def create_indices_metadata_table(
    computation_db: DatabaseResource, metadata_table: PostgresTableIdentifier
):
    computation_db.connection.send_query(
        SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING gist (boundary)").format(
            Identifier(f"{metadata_table.table.str}_boundary_index"), metadata_table.id
        )
    )
    computation_db.connection.send_query(
        SQL(
            "CREATE INDEX IF NOT EXISTS {} ON {} (hash) WHERE (hash IS NOT NULL);"
        ).format(
            Identifier(f"{metadata_table.table.str}_hash_index"), metadata_table.id
        )
    )
    computation_db.connection.send_query(
        SQL(
            "CREATE INDEX IF NOT EXISTS {} ON {} USING gin ((pdal_info -> 'filename') jsonb_path_ops) WHERE ((pdal_info -> 'filename') IS DISTINCT FROM jsonb('\"\"'))"
        ).format(
            Identifier(f"{metadata_table.table.str}_filename_index"), metadata_table.id
        )
    )


def compute_load_metadata(
    tile_id: str,
    config: MetadataConfig,
    laz_files_ahn,
    metadata_table_ahn,
    tile_index_ahn_pdok,
    computation_db: DatabaseResource,
    pdal: PDALResource,
) -> Output[None]:
    """Metadata of the AHN LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'. The metadata is loaded into the metadata database table.

    Args:
        tile_id (str): The ID of the tile.
        config (MetadataConfig): Asset configuration.
        laz_files_ahn (LAZDownload): The LAZ file download result, produced by the
            `laz_files_ahn*` asset.
        metadata_table_ahn (PostgresTableIdentifier): The metadata database table
            indentifier.
        tile_index_ahn_pdok (dict): Downloaded with `download_ahn_index`.
        computation_db (DatabaseResource): Database connection resource.
        pdal (PDALResource): PDAL resource for executing pdal info.

    Returns:
        None
    """
    logger = get_dagster_logger()
    conn = computation_db.connection
    if not laz_files_ahn.new:
        if not config.force:
            logger.info(
                f"Metadata for this LAZ tile {tile_id} already exists, "
                f"skipping computation."
            )
            return Output(None)

    ret_code, out_info = pdal_info(
        pdal.runner,
        file_path=laz_files_ahn.path,
        with_all=config.all,
    )

    set_json_dumps(dumps=partial(json.dumps, ensure_ascii=False))

    query_params = {
        "metadata_table": metadata_table_ahn.id,
        "tile_id": Literal(tile_id),
        "hash": Literal(f"{laz_files_ahn.hash_name}:{laz_files_ahn.hash_hexdigest}"),
        "insert_time": Literal(datetime.now(tz=pytz.timezone("Europe/Amsterdam"))),
        "pdal_info": Jsonb(out_info),
        "boundary": Literal(json.dumps(tile_index_ahn_pdok[tile_id]["geometry"])),
    }
    query = SQL("""
        INSERT INTO {metadata_table}(
            tile_id,
            hash,
            insert_time,
            pdal_info,
            boundary
        )
        VALUES (
            {tile_id},
            {hash},
            {insert_time},
            {pdal_info},
            ST_SetSRID(ST_GeomFromGeoJSON({boundary}), 28992)
        );
        """).format(**query_params)
    logger.debug(conn.print_query(query))
    conn.send_query(query)
    # Cannot index the table here, because this is a partitioned assed. This means that
    # this function is called for each partition, which would index the table after
    # each partition.
    return Output(None, metadata={**out_info})


def metadata_table_ahn(
    computation_db: DatabaseResource, ahn_version: int
) -> PostgresTableIdentifier:
    logger = get_dagster_logger()
    conn = computation_db.connection
    new_schema = "ahn"
    create_schema(computation_db, new_schema, logger=logger)
    new_table = PostgresTableIdentifier(new_schema, f"metadata_ahn{ahn_version}")
    query = load_sql(query_params={"new_table": new_table})
    logger.info(conn.print_query(query))
    conn.send_query(query)
    return new_table
