from dagster import asset, Output, AssetIn
from psycopg.sql import SQL

from bag3d.common.utils.database import (
    create_schema,
    load_sql,
    postgrestable_from_query,
)
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.resources.database import DatabaseResource
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA


@asset(
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "bag_kas_warenhuis": AssetIn(key_prefix="intermediary"),
        "bag_bag_overlap": AssetIn(key_prefix="intermediary"),
    },
    op_tags={"compute_kind": "sql"},
)
def reconstruction_input(
    context,
    bag_pandactueelbestaand,
    bag_kas_warenhuis,
    bag_bag_overlap,
    db_connection: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """The input for the building reconstruction, where:
    - duplicates are removed
    """
    create_schema(db_connection, RECONSTRUCTION_INPUT_SCHEMA, logger=context.log)
    new_table = PostgresTableIdentifier(
        RECONSTRUCTION_INPUT_SCHEMA, "reconstruction_input"
    )
    query = load_sql(
        query_params={
            "bag_cleaned": bag_pandactueelbestaand,
            "bag_kas_warenhuis": bag_kas_warenhuis,
            "bag_bag_overlap": bag_bag_overlap,
            "new_table": new_table,
        }
    )
    metadata = postgrestable_from_query(db_connection, query, new_table, logger=context.log)
    db_connection.connect.send_query(
        SQL("ALTER TABLE {new_table} ADD PRIMARY KEY (fid)"),
        query_params={"new_table": new_table},
    )
    return Output(new_table, metadata=metadata)
