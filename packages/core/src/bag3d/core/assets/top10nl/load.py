from dagster import (asset, Output)

from bag3d.common.utils.database import load_sql, postgrestable_from_query, drop_table, \
    create_schema
from bag3d.common.utils.geodata import ogr2postgres
from bag3d.common.custom_types import PostgresTableIdentifier


@asset(required_resource_keys={"db_connection", "gdal"})
def stage_top10nl_gebouw(context, extract_top10nl) -> Output[PostgresTableIdentifier]:
    """The TOP10NL Gebouw layer, loaded as-is from the extract."""
    new_schema = "stage_top10nl"
    create_schema(context, context.resources.db_connection, new_schema)
    xsd = "https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd"
    new_table = PostgresTableIdentifier(new_schema, "gebouw")
    # Need to explicitly drop the table just in case (...couz GDAL...)
    drop_table(context, context.resources.db_connection, new_table)
    metadata = ogr2postgres(context=context, dataset="top10nl", xsd=xsd,
                            extract_path=extract_top10nl, feature_type="gebouw",
                            new_table=new_table)
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def top10nl_gebouw(context, stage_top10nl_gebouw) -> Output[PostgresTableIdentifier]:
    """The cleaned TOP10NL Gebouw polygon layer that only contains the current
    (timely) and physically existing buildings."""
    new_schema = "top10nl"
    create_schema(context, context.resources.db_connection, new_schema)
    table_name = "gebouw"
    new_table = PostgresTableIdentifier("top10nl", table_name)
    query = load_sql(query_params={"gebouw_tbl": stage_top10nl_gebouw,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    context.resources.db_connection.send_query(
        f"ALTER TABLE {new_table} ADD PRIMARY KEY (fid)")
    geom_idx_name = f"{table_name}_geometrie_vlak_idx"
    context.resources.db_connection.send_query(
        f"CREATE INDEX {geom_idx_name} ON {new_table} USING gist (geometrie_vlak)")
    context.resources.db_connection.send_query(
        f"CREATE INDEX {table_name}_typegebouw_idx ON {new_table} USING gin (typegebouw)")
    context.resources.db_connection.send_query(
        f"CLUSTER {new_table} USING {geom_idx_name}")
    return Output(new_table, metadata=metadata)
