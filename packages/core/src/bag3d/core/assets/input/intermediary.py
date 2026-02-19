from dagster import asset, Output, AssetIn, get_dagster_logger, AutomationCondition

from bag3d.common.utils.database import (
    create_schema,
    load_sql,
    postgrestable_from_query,
)
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.resources.database import DatabaseResource

INTERMEDIARY = "intermediary"
NEW_SCHEMA = "reconstruction_input"

logger = get_dagster_logger("input.intermediary")


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "top10nl_gebouw": AssetIn(key_prefix="top10nl"),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_kas_warenhuis(
    bag_pandactueelbestaand, top10nl_gebouw, db_connection: DatabaseResource
):
    """The BAG Pand labelled as greenhouse, warehouse (kas, warenhuis) using the
    TOP10NL."""
    create_schema(db_connection, NEW_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "bag_kas_warenhuis")
    query = load_sql(
        query_params={
            "bag_cleaned": bag_pandactueelbestaand,
            "top10nl_gebouw": top10nl_gebouw,
            "new_table": new_table,
        }
    )
    metadata = postgrestable_from_query(db_connection, query, new_table, logger=logger)
    db_connection.connection.send_query(
        f"ALTER TABLE {new_table} ADD PRIMARY KEY (fid)"
    )
    return Output(new_table, metadata=metadata)


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_bag_overlap(bag_pandactueelbestaand, db_connection: DatabaseResource):
    """The overlap between BAG polygons, in m2. For every object the
    total area of overlap is calculated."""
    create_schema(db_connection, NEW_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "bag_bag_overlap")
    query = load_sql(
        query_params={"bag_cleaned": bag_pandactueelbestaand, "new_table": new_table}
    )
    metadata = postgrestable_from_query(db_connection, query, new_table, logger=logger)
    db_connection.connection.send_query(
        f"ALTER TABLE {new_table} ADD PRIMARY KEY (fid)"
    )
    return Output(new_table, metadata=metadata)
