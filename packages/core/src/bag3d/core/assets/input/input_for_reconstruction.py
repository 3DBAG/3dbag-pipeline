from dagster import asset, Output, AssetIn
from psycopg.sql import SQL

from bag3d.common.utils.database import create_schema, load_sql, \
    postgrestable_from_query
from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA


@asset(
    required_resource_keys={"db_connection"},
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "bag_kas_warenhuis": AssetIn(key_prefix="intermediary"),
        "bag_bag_overlap": AssetIn(key_prefix="intermediary"),
    },
    op_tags={"kind": "sql"}
)
def reconstruction_input(context, bag_pandactueelbestaand, bag_kas_warenhuis, bag_bag_overlap):
    """The input for the building reconstruction, where:
    - duplicates are removed
    """
    create_schema(context, RECONSTRUCTION_INPUT_SCHEMA)
    new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA,
                                        "reconstruction_input")
    query = load_sql(query_params={"bag_cleaned": bag_pandactueelbestaand,
                                   "bag_kas_warenhuis": bag_kas_warenhuis,
                                   "bag_bag_overlap": bag_bag_overlap,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    context.resources.db_connection.send_query(
        SQL("ALTER TABLE {new_table} ADD PRIMARY KEY (fid)"),
        query_params={"new_table": new_table}
    )
    return Output(new_table, metadata=metadata)


# @asset(
#     required_resource_keys={"db_connection"},
#     ins={
#         "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
#         "bag_kas_warenhuis": AssetIn(key_prefix="intermediary")
#     },
#     op_tags={"kind": "sql"}
# )
# def reconstruction_excluded(context, bag_pandactueelbestaand, bag_kas_warenhuis):
#     """The BAG footprints that were excluded from the reconstruction, because they are
#         1) newer than their point cloud, or
#         2) completely underground, or
#         3) floating above other buildings.
#     """
#     create_schema(context, RECONSTRUCTION_INPUT_SCHEMA)
#     new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "reconstruction_excluded")
#     query = load_sql(query_params={"bag_cleaned": bag_pandactueelbestaand,
#                                    "bag_kas_warenhuis": bag_kas_warenhuis,
#                                    "new_table": new_table})
#     metadata = postgrestable_from_query(context, query, new_table)
#     return Output(new_table, metadata=metadata)
