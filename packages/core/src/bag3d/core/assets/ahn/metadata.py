import json
from datetime import datetime

import pytz
from dagster import asset, Output, Field
from pgutils import PostgresTableIdentifier
from psycopg.sql import Literal, SQL

from bag3d.common.utils.geodata import pdal_info
from bag3d.common.utils.database import create_schema, load_sql
from bag3d.core.assets.ahn.core import PartitionDefinitionAHN


@asset(required_resource_keys={"db_connection"})
def metadata_table_ahn3(context):
    """A metadata table for the AHN3, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(context, ahn_version=3)


@asset(required_resource_keys={"db_connection"})
def metadata_table_ahn4(context):
    """A metadata table for the AHN4, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(context, ahn_version=4)


@asset(required_resource_keys={"db_connection"})
def metadata_table_ahn5(context):
    """A metadata table for the AHN5, including the tile boundaries, tile IDs etc."""
    return metadata_table_ahn(context, ahn_version=5)


@asset(
    config_schema={
        "all": Field(
            bool, default_value=True, description="Run `pdal info` with `--all`."
        ),
        "force": Field(
            bool,
            default_value=True,
            description="Force the re-compute of the metadata.",
        ),
    },
    required_resource_keys={"pdal", "db_connection"},
    partitions_def=PartitionDefinitionAHN(ahn_version=3),
)
def metadata_ahn3(context, laz_files_ahn3, metadata_table_ahn3, tile_index_pdok):
    """Metadata of the AHN3 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context, laz_files_ahn3, metadata_table_ahn3, tile_index_pdok
    )


@asset(
    config_schema={
        "all": Field(
            bool, default_value=True, description="Run `pdal info` with `--all`."
        ),
        "force": Field(
            bool,
            default_value=True,
            description="Force the re-compute of the metadata.",
        ),
    },
    required_resource_keys={"pdal", "db_connection"},
    partitions_def=PartitionDefinitionAHN(ahn_version=4),
)
def metadata_ahn4(context, laz_files_ahn4, metadata_table_ahn4, tile_index_pdok):
    """Metadata of the AHN4 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context, laz_files_ahn4, metadata_table_ahn4, tile_index_pdok
    )


@asset(
    config_schema={
        "all": Field(
            bool, default_value=True, description="Run `pdal info` with `--all`."
        ),
        "force": Field(
            bool,
            default_value=True,
            description="Force the re-compute of the metadata.",
        ),
    },
    required_resource_keys={"pdal", "db_connection"},
    partitions_def=PartitionDefinitionAHN(ahn_version=5),
)
def metadata_ahn5(context, laz_files_ahn5, metadata_table_ahn5, tile_index_pdok):
    """Metadata of the AHN5 LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'.
    The metadata is loaded into the metadata database table."""
    return compute_load_metadata(
        context, laz_files_ahn5, metadata_table_ahn5, tile_index_pdok
    )


# TODO: add some op or sensor or sth that indexes and clusters the metadata table after
#   the partitioned job is completed. Keep in mind that some partitions might fail, but
#   we still need to index the table.
# context.resources.db_connection.send_query(f"ALTER TABLE {metadata_table_ahn} ADD PRIMARY KEY (tile_id)")
# geom_idx_name = f"{metadata_table_ahn.table}_boundary_idx"
# context.resources.db_connection.send_query(
#     f"CREATE INDEX {geom_idx_name} ON {metadata_table_ahn} USING gist (boundary)")
# context.resources.db_connection.send_query(f"CLUSTER {metadata_table_ahn} USING {geom_idx_name}")


def compute_load_metadata(
    context, laz_files_ahn, metadata_table_ahn, tile_index_ahn_pdok
):
    """Metadata of the AHN LAZ file, retrieved from the PDOK tile index and
    computed with 'pdal info'. The metadata is loaded into the metadata database table.

    Args:
        context (OpExecutionContext): Op context.
        laz_files_ahn (LAZDownload): The LAZ file download result, produced by the
            `laz_files_ahn*` asset.
        metadata_table_ahn (PostgresTableIdentifier): The metadata database table
            indentifier.
        tile_index_ahn_pdok (dict): Downloaded with `download_ahn_index`.

    Returns:

    """
    tile_id = context.partition_key
    conn = context.resources.db_connection
    if not laz_files_ahn.new:
        if not context.op_config["force"]:
            context.log.info(
                f"Metadata for AHN3 LAZ tile {tile_id} already exists, "
                f"skipping computation."
            )
            return Output(None)
    try:
        ret_code, out_info = pdal_info(
            context.resources.pdal,
            file_path=laz_files_ahn.path,
            with_all=context.op_config["all"],
        )
        if ret_code != 0:
            raise
    # if pdal fails store nothing in the table
    except Exception:
        out_info = None

    query_params = {
        "metadata_table": metadata_table_ahn.id,
        "tile_id": Literal(tile_id),
        "hash": Literal(f"{laz_files_ahn.hash_name}:{laz_files_ahn.hash_hexdigest}"),
        "download_time": Literal(datetime.now(tz=pytz.timezone("Europe/Amsterdam"))),
        "pdal_info": Literal(json.dumps(out_info)),
        "boundary": Literal(json.dumps(tile_index_ahn_pdok[tile_id]["geometry"])),
    }
    query = SQL("""
        INSERT INTO {metadata_table}(
            tile_id,
            hash,
            download_time,
            pdal_info,
            boundary
        ) 
        VALUES (
            {tile_id}, 
            {hash}, 
            {download_time}, 
            {pdal_info}, 
            ST_SetSRID(ST_GeomFromGeoJSON({boundary}), 28992)
        );    
        """).format(**query_params)
    context.log.info(conn.print_query(query))
    conn.send_query(query)
    # Cannot index the table here, because this is a partitioned assed. This means that
    # this function is called for each partition, which would index the table after
    # each partition.
    return Output(None, metadata={**out_info})


def metadata_table_ahn(context, ahn_version: int) -> PostgresTableIdentifier:
    conn = context.resources.db_connection
    new_schema = "ahn"
    create_schema(context, new_schema)
    new_table = PostgresTableIdentifier(new_schema, f"metadata_ahn{ahn_version}")
    query = load_sql(query_params={"new_table": new_table})
    context.log.info(conn.print_query(query))
    conn.send_query(query)
    return new_table
