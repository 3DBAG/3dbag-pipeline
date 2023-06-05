from dagster import (asset, Output, AssetIn)

from bag3d_pipeline.core import create_schema, load_sql, postgrestable_from_query
from bag3d_pipeline.custom_types import PostgresTableIdentifier

INTERMEDIARY = "intermediary"
NEW_SCHEMA = "reconstruction_input"


@asset(
    required_resource_keys={"db_connection"},
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "top10nl_gebouw": AssetIn(key_prefix="top10nl")
    },
    op_tags={"kind": "sql"}
)
def bag_kas_warenhuis(context, bag_pandactueelbestaand, top10nl_gebouw):
    """The BAG Pand labelled as greenhouse, warehouse (kas, warenhuis) using the
    TOP10NL."""
    create_schema(context, context.resources.db_connection, NEW_SCHEMA)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "bag_kas_warenhuis")
    query = load_sql(query_params={"bag_cleaned": bag_pandactueelbestaand,
                                   "top10nl_gebouw": top10nl_gebouw,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    context.resources.db_connection.send_query(
        f"ALTER TABLE {new_table} ADD PRIMARY KEY (fid)")
    return Output(new_table, metadata=metadata)
