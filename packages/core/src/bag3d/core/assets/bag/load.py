from dagster import (asset, Output)

from bag3d.common.utils.database import load_sql, postgrestable_from_query, \
    create_schema
from bag3d.common.types import PostgresTableIdentifier

NEW_SCHEMA = "lvbag"


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def bag_woonplaatsactueelbestaand(context, stage_bag_woonplaats):
    """The BAG Woonplaats layer that only contains the current (timely) and physically
    existing objects."""
    create_schema(context, NEW_SCHEMA)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "woonplaatsactueelbestaand")
    query = load_sql(query_params={"wpl_tbl": stage_bag_woonplaats,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def bag_verblijfsobjectactueelbestaand(context, stage_bag_verblijfsobject):
    """The BAG Verblijfsobject layer that only contains the current (timely) and
    physically existing buildings."""
    create_schema(context, NEW_SCHEMA)
    table_name = "verblijfsobjectactueelbestaand"
    new_table = PostgresTableIdentifier(NEW_SCHEMA, table_name)
    query = load_sql(query_params={"vbo_tbl": stage_bag_verblijfsobject,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    context.resources.db_connection.send_query(
        f"ALTER TABLE {new_table} ADD PRIMARY KEY (fid)")
    context.resources.db_connection.send_query(
        f"CREATE INDEX {table_name}_geometrie_idx ON {new_table} USING gist (geometrie)")
    context.resources.db_connection.send_query(
        f"CREATE INDEX {table_name}_identificatie_idx ON {new_table} (identificatie)")
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def bag_pandactueelbestaand(context, stage_bag_pand):
    """The BAG Pand layer that only contains the current (timely) and physically
    existing buildings."""
    create_schema(context, NEW_SCHEMA)
    table_name = "pandactueelbestaand"
    new_table = PostgresTableIdentifier(NEW_SCHEMA, table_name)
    query = load_sql(query_params={"pand_tbl": stage_bag_pand,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    context.resources.db_connection.send_query(
        f"ALTER TABLE {new_table} ADD PRIMARY KEY (fid)")
    geom_idx_name = f"{table_name}_geometrie_idx"
    context.resources.db_connection.send_query(
        f"CREATE INDEX {geom_idx_name} ON {new_table} USING gist (geometrie)")
    context.resources.db_connection.send_query(
        f"CREATE INDEX {table_name}_identificatie_idx ON {new_table} (identificatie)")
    context.resources.db_connection.send_query(
        f"CLUSTER {new_table} USING {geom_idx_name}")
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def bag_openbareruimteactueelbestaand(context, stage_bag_openbareruimte):
    """The BAG Pand layer that only contains the current (timely) and physically
    existing objects."""
    create_schema(context, NEW_SCHEMA)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "openbareruimteactueelbestaand")
    query = load_sql(query_params={"opr_tbl": stage_bag_openbareruimte,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def bag_nummeraanduidingactueelbestaand(context, stage_bag_nummeraanduiding):
    """The BAG Nummeraanduiding layer that only contains the current (timely) and
    physically existing objects."""
    create_schema(context, NEW_SCHEMA)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "nummeraanduidingactueelbestaand")
    query = load_sql(query_params={"num_tbl": stage_bag_nummeraanduiding,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)
