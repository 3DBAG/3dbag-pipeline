import json
from datetime import datetime
from functools import partial

import pytz
from dagster import asset, Output, Config, get_dagster_logger
from pgutils import PostgresTableIdentifier
from psycopg.sql import Literal, SQL
from psycopg.types.json import Jsonb, set_json_dumps
from pydantic import Field

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import PDALResource
from bag3d.common.types import PostgresTable
from bag3d.common.utils.geodata import pdal_info
from bag3d.common.utils.database import create_schema, load_sql
from bag3d.core.assets.ahn.core import partition_definition_ahn


class MetadataConfig(Config):
    """Configuration for AHN metadata assets."""

    all: bool = Field(default=True, description="Run `pdal info` with `--all`.")
    force: bool = Field(
        default=True, description="Force the re-compute of the metadata."
    )
    verbose: bool = Field(default=False, description="Output stdout/stderr from pdal")


@asset
def metadata_table_ahn3(
    context, db_connection: DatabaseResource
) -> PostgresTableIdentifier:
    """A metadata table for the AHN3, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(context, db_connection, ahn_version=3)


@asset
def metadata_table_ahn4(context, db_connection: DatabaseResource):
    """A metadata table for the AHN4, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(context, db_connection, ahn_version=4)


@asset
def metadata_table_ahn5(context, db_connection: DatabaseResource):
    """A metadata table for the AHN5, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(context, db_connection, ahn_version=5)


@asset(partitions_def=partition_definition_ahn)
def metadata_ahn3(
    context,
    config: MetadataConfig,
    laz_files_ahn3,
    metadata_table_ahn3,
    tile_index_ahn,
    db_connection: DatabaseResource,
    pdal: PDALResource,
):
    """Metadata of the AHN3 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context,
        config,
        laz_files_ahn3,
        metadata_table_ahn3,
        tile_index_ahn,
        db_connection,
        pdal,
        verbose=config.verbose,
    )


@asset(partitions_def=partition_definition_ahn)
def metadata_ahn4(
    context,
    config: MetadataConfig,
    laz_files_ahn4,
    metadata_table_ahn4,
    tile_index_ahn,
    db_connection: DatabaseResource,
    pdal: PDALResource,
):
    """Metadata of the AHN4 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context,
        config,
        laz_files_ahn4,
        metadata_table_ahn4,
        tile_index_ahn,
        db_connection,
        pdal,
        verbose=config.verbose,
    )


@asset(partitions_def=partition_definition_ahn)
def metadata_ahn5(
    context,
    config: MetadataConfig,
    laz_files_ahn5,
    metadata_table_ahn5,
    tile_index_ahn,
    db_connection: DatabaseResource,
    pdal: PDALResource,
):
    """Metadata of the AHN5 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context,
        config,
        laz_files_ahn5,
        metadata_table_ahn5,
        tile_index_ahn,
        db_connection,
        pdal,
        verbose=config.verbose,
    )


@asset(deps=["metadata_ahn3"])
def metadata_ahn3_index(
    db_connection: DatabaseResource,
    metadata_table_ahn3: PostgresTable,
):
    """Create indices on the AHN3 metadata table."""
    create_indices_metadata_table(db_connection, metadata_table_ahn3)
    return metadata_table_ahn3


@asset(deps=["metadata_ahn4"])
def metadata_ahn4_index(
    db_connection: DatabaseResource,
    metadata_table_ahn4: PostgresTable,
):
    """Create indices on the AHN4 metadata table."""
    create_indices_metadata_table(db_connection, metadata_table_ahn4)
    return metadata_table_ahn4


@asset(deps=["metadata_ahn5"])
def metadata_ahn5_index(
    db_connection: DatabaseResource,
    metadata_table_ahn5: PostgresTable,
):
    """Create indices on the AHN5 metadata table."""
    create_indices_metadata_table(db_connection, metadata_table_ahn5)
    return metadata_table_ahn5


def create_indices_metadata_table(
    db_connection: DatabaseResource, metadata_table: PostgresTable
):
    db_connection.connect.send_query(
        f"CREATE INDEX IF NOT EXISTS {metadata_table.table}_boundary_index ON {metadata_table} USING gist (boundary)"
    )
    db_connection.connect.send_query(
        f"CREATE INDEX IF NOT EXISTS {metadata_table.table}_hash_index ON {metadata_table} (hash) WHERE (hash IS NOT NULL);"
    )
    db_connection.connect.send_query(
        f"CREATE INDEX IF NOT EXISTS {metadata_table.table}_filename_index ON {metadata_table} USING gin ((pdal_info -> 'filename') jsonb_path_ops) WHERE ((pdal_info -> 'filename') IS DISTINCT FROM jsonb('\"\"'))"
    )


def compute_load_metadata(
    context,
    config,
    laz_files_ahn,
    metadata_table_ahn,
    tile_index_ahn_pdok,
    db_connection: DatabaseResource,
    pdal: PDALResource,
    verbose: bool = False,
):
    """Metadata of the AHN LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'. The metadata is loaded into the metadata database table.

    Args:
        context (OpExecutionContext): Op context.
        config (MetadataConfig): Asset configuration.
        laz_files_ahn (LAZDownload): The LAZ file download result, produced by the
            `laz_files_ahn*` asset.
        metadata_table_ahn (PostgresTableIdentifier): The metadata database table
            indentifier.
        tile_index_ahn_pdok (dict): Downloaded with `download_ahn_index`.
        db_connection (DatabaseResource): Database connection resource.
        pdal (PDALResource): PDAL resource for executing pdal info.
        verbose (bool): Forward the stdout/stderr from pdal.

    Returns:
        None
    """
    logger = get_dagster_logger()
    tile_id = context.partition_key
    conn = db_connection.connect
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
        verbose=verbose,
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
    context, db_connection: DatabaseResource, ahn_version: int
) -> PostgresTableIdentifier:
    logger = get_dagster_logger()
    conn = db_connection.connect
    new_schema = "ahn"
    create_schema(db_connection, new_schema, logger=logger)
    new_table = PostgresTableIdentifier(new_schema, f"metadata_ahn{ahn_version}")
    query = load_sql(query_params={"new_table": new_table})
    logger.info(conn.print_query(query))
    conn.send_query(query)
    return new_table
