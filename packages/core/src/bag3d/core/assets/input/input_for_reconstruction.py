from bag3d.common.resources.database import DatabaseResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import (
    create_schema,
    load_sql,
    postgrestable_from_query,
)
from dagster import AssetIn, AutomationCondition, Output, asset, get_dagster_logger
from psycopg.sql import SQL

from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA

logger = get_dagster_logger("input.input_for_reconstruction")


@asset(
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "bag_kas_warenhuis": AssetIn(key_prefix="intermediary"),
        "bag_bag_overlap": AssetIn(key_prefix="intermediary"),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def reconstruction_input(
    bag_pandactueelbestaand,
    bag_kas_warenhuis,
    bag_bag_overlap,
    computation_db: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """The input for the building reconstruction, where:
    - duplicates are removed
    """
    create_schema(computation_db, RECONSTRUCTION_INPUT_SCHEMA, logger=logger)
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
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    computation_db.connection.send_query(
        SQL("ALTER TABLE {new_table} ADD PRIMARY KEY (fid)"),
        query_params={"new_table": new_table},
    )
    return Output(new_table, metadata=metadata)
