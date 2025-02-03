from datetime import datetime
from dagster import asset, Output, Field

from bag3d.common.utils.database import (
    load_sql,
    postgrestable_from_query,
    create_schema,
)
from bag3d.common.types import PostgresTableIdentifier

NEW_SCHEMA = "lvbag"


@asset(
    required_resource_keys={"db_connection"},
    op_tags={"compute_kind": "sql"},
    config_schema={
        "pelidatum": Field(
            str, description="Reference date in format YYYY-MM-DD.", is_required=False
        ),
    },
)
def bag_woonplaatsactueelbestaand(context, stage_bag_woonplaats):
    """The BAG Woonplaats layer that only contains the current (timely) and physically
    existing objects."""
    create_schema(context, NEW_SCHEMA)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "woonplaatsactueelbestaand")
    if (p := context.op_execution_context.op_config.get("pelidatum")) is not None:
        pelidatum = datetime.strptime(p, "%Y-%m-%d")
    else:
        pelidatum = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "wpl_tbl": stage_bag_woonplaats,
            "new_table": new_table,
            "pelidatum": pelidatum,
        },
    )
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)


@asset(
    required_resource_keys={"db_connection"},
    op_tags={"compute_kind": "sql"},
    config_schema={
        "pelidatum": Field(
            str, description="Reference date in format YYYY-MM-DD.", is_required=False
        ),
    },
)
def bag_verblijfsobjectactueelbestaand(context, stage_bag_verblijfsobject):
    """The BAG Verblijfsobject layer that only contains the current (timely) and
    physically existing buildings. The data can be limited to a specific reference date by setting
    the *pelidatum* parameter."""
    create_schema(context, NEW_SCHEMA)
    table_name = "verblijfsobjectactueelbestaand"
    new_table = PostgresTableIdentifier(NEW_SCHEMA, table_name)
    if (p := context.op_execution_context.op_config.get("pelidatum")) is not None:
        pelidatum = datetime.strptime(p, "%Y-%m-%d")
    else:
        pelidatum = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "vbo_tbl": stage_bag_verblijfsobject,
            "new_table": new_table,
            "pelidatum": pelidatum,
        }
    )
    metadata = postgrestable_from_query(context, query, new_table)
    context.resources.db_connection.connect.send_query(
        f"ALTER TABLE {new_table} ADD PRIMARY KEY (fid)"
    )
    context.resources.db_connection.connect.send_query(
        f"CREATE INDEX {table_name}_geometrie_idx ON {new_table} USING gist (geometrie)"
    )
    context.resources.db_connection.connect.send_query(
        f"CREATE INDEX {table_name}_identificatie_idx ON {new_table} (identificatie)"
    )
    return Output(new_table, metadata=metadata)


@asset(
    required_resource_keys={"db_connection"},
    op_tags={"compute_kind": "sql"},
    config_schema={
        "pelidatum": Field(
            str, description="Reference date in format YYYY-MM-DD.", is_required=False
        ),
    },
)
def bag_pandactueelbestaand(context, stage_bag_pand):
    """The BAG Pand layer that only contains the current (timely) and physically
    existing buildings. The data can be limited to a specific reference date by setting
    the *pelidatum* parameter."""
    create_schema(context, NEW_SCHEMA)
    table_name = "pandactueelbestaand"
    new_table = PostgresTableIdentifier(NEW_SCHEMA, table_name)
    if (p := context.op_execution_context.op_config.get("pelidatum")) is not None:
        pelidatum = datetime.strptime(p, "%Y-%m-%d")
    else:
        pelidatum = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "pand_tbl": stage_bag_pand,
            "new_table": new_table,
            "pelidatum": pelidatum,
        }
    )
    metadata = postgrestable_from_query(context, query, new_table)
    context.resources.db_connection.connect.send_query(
        f"ALTER TABLE {new_table} ADD PRIMARY KEY (fid)"
    )
    geom_idx_name = f"{table_name}_geometrie_idx"
    context.resources.db_connection.connect.send_query(
        f"CREATE INDEX {geom_idx_name} ON {new_table} USING gist (geometrie)"
    )
    context.resources.db_connection.connect.send_query(
        f"CREATE INDEX {table_name}_identificatie_idx ON {new_table} (identificatie)"
    )
    context.resources.db_connection.connect.send_query(
        f"CLUSTER {new_table} USING {geom_idx_name}"
    )
    return Output(new_table, metadata=metadata)


@asset(
    required_resource_keys={"db_connection"},
    op_tags={"compute_kind": "sql"},
    config_schema={
        "pelidatum": Field(
            str, description="Reference date in format YYYY-MM-DD.", is_required=False
        ),
    },
)
def bag_openbareruimteactueelbestaand(context, stage_bag_openbareruimte):
    """The BAG Pand layer that only contains the current (timely) and physically
    existing objects. The data can be limited to a specific reference date by setting
    the *pelidatum* parameter."""
    create_schema(context, NEW_SCHEMA)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "openbareruimteactueelbestaand")
    if (p := context.op_execution_context.op_config.get("pelidatum")) is not None:
        pelidatum = datetime.strptime(p, "%Y-%m-%d")
    else:
        pelidatum = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "opr_tbl": stage_bag_openbareruimte,
            "new_table": new_table,
            "pelidatum": pelidatum,
        },
    )
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)


@asset(
    required_resource_keys={"db_connection"},
    op_tags={"compute_kind": "sql"},
    config_schema={
        "pelidatum": Field(
            str, description="Reference date in format YYYY-MM-DD.", is_required=False
        ),
    },
)
def bag_nummeraanduidingactueelbestaand(context, stage_bag_nummeraanduiding):
    """The BAG Nummeraanduiding layer that only contains the current (timely) and
    physically existing objects. The data can be limited to a specific reference date by setting
    the *pelidatum* parameter."""
    create_schema(context, NEW_SCHEMA)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "nummeraanduidingactueelbestaand")
    if (p := context.op_execution_context.op_config.get("pelidatum")) is not None:
        pelidatum = datetime.strptime(p, "%Y-%m-%d")
    else:
        pelidatum = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "num_tbl": stage_bag_nummeraanduiding,
            "new_table": new_table,
            "pelidatum": pelidatum,
        }
    )
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)
