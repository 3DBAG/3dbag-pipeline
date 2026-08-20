from datetime import UTC, datetime

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import (
    create_schema,
    load_sql,
    postgrestable_from_query,
)
from dagster import AutomationCondition, Config, Output, asset, get_dagster_logger
from psycopg.sql import SQL, Identifier
from pydantic import Field

NEW_SCHEMA = "lvbag"
logger = get_dagster_logger("bag.load")


class BagLoadConfig(Config):
    """Configuration for BAG load assets."""

    reference_date: str | None = Field(
        default=None, description="Reference date in format YYYY-MM-DD."
    )


@asset(
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_woonplaatsactueelbestaand(
    config: BagLoadConfig,
    computation_db: DatabaseResource,
    stage_bag_woonplaats,
) -> Output[PostgresTableIdentifier]:
    """The BAG Woonplaats layer that only contains the current (timely) and physically
    existing objects."""
    create_schema(computation_db, NEW_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "woonplaatsactueelbestaand")
    if config.reference_date is not None:
        reference_date = datetime.strptime(config.reference_date, "%Y-%m-%d").replace(
            tzinfo=UTC
        )
    else:
        reference_date = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "wpl_tbl": stage_bag_woonplaats,
            "new_table": new_table,
            "reference_date": reference_date,
        },
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    return Output(new_table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_verblijfsobjectactueelbestaand(
    config: BagLoadConfig,
    computation_db: DatabaseResource,
    stage_bag_verblijfsobject,
) -> Output[PostgresTableIdentifier]:
    """The BAG Verblijfsobject layer that only contains the current (timely) and
    physically existing buildings. The data can be limited to a specific reference date by setting
    the *reference_date* parameter."""
    create_schema(computation_db, NEW_SCHEMA, logger=logger)
    table_name = "verblijfsobjectactueelbestaand"
    new_table = PostgresTableIdentifier(NEW_SCHEMA, table_name)
    if config.reference_date is not None:
        reference_date = datetime.strptime(config.reference_date, "%Y-%m-%d").replace(
            tzinfo=UTC
        )
    else:
        reference_date = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "vbo_tbl": stage_bag_verblijfsobject,
            "new_table": new_table,
            "reference_date": reference_date,
        }
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    computation_db.connection.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY (fid)").format(new_table.id)
    )
    computation_db.connection.send_query(
        SQL("CREATE INDEX {} ON {} USING gist (geometrie)").format(
            Identifier(f"{table_name}_geometrie_idx"), new_table.id
        )
    )
    computation_db.connection.send_query(
        SQL("CREATE INDEX {} ON {} (identificatie)").format(
            Identifier(f"{table_name}_identificatie_idx"), new_table.id
        )
    )
    return Output(new_table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_pandactueelbestaand(
    config: BagLoadConfig, computation_db: DatabaseResource, stage_bag_pand
) -> Output[PostgresTableIdentifier]:
    """The BAG Pand layer that only contains the current (timely) and physically
    existing buildings. The data can be limited to a specific reference date by setting
    the *reference_date* parameter."""
    create_schema(computation_db, NEW_SCHEMA, logger=logger)
    table_name = "pandactueelbestaand"
    new_table = PostgresTableIdentifier(NEW_SCHEMA, table_name)
    if config.reference_date is not None:
        reference_date = datetime.strptime(config.reference_date, "%Y-%m-%d").replace(
            tzinfo=UTC
        )
    else:
        reference_date = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "pand_tbl": stage_bag_pand,
            "new_table": new_table,
            "reference_date": reference_date,
        }
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    computation_db.connection.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY (fid)").format(new_table.id)
    )
    geom_idx_name = f"{table_name}_geometrie_idx"
    computation_db.connection.send_query(
        SQL("CREATE INDEX {} ON {} USING gist (geometrie)").format(
            Identifier(geom_idx_name), new_table.id
        )
    )
    computation_db.connection.send_query(
        SQL("CREATE INDEX {} ON {} (identificatie)").format(
            Identifier(f"{table_name}_identificatie_idx"), new_table.id
        )
    )
    computation_db.connection.send_query(
        SQL("CLUSTER {} USING {}").format(new_table.id, Identifier(geom_idx_name))
    )
    return Output(new_table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_openbareruimteactueelbestaand(
    config: BagLoadConfig,
    computation_db: DatabaseResource,
    stage_bag_openbareruimte,
) -> Output[PostgresTableIdentifier]:
    """The BAG Pand layer that only contains the current (timely) and physically
    existing objects. The data can be limited to a specific reference date by setting
    the *reference_date* parameter."""
    create_schema(computation_db, NEW_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "openbareruimteactueelbestaand")
    if config.reference_date is not None:
        reference_date = datetime.strptime(config.reference_date, "%Y-%m-%d").replace(
            tzinfo=UTC
        )
    else:
        reference_date = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "opr_tbl": stage_bag_openbareruimte,
            "new_table": new_table,
            "reference_date": reference_date,
        },
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    return Output(new_table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_nummeraanduidingactueelbestaand(
    config: BagLoadConfig,
    computation_db: DatabaseResource,
    stage_bag_nummeraanduiding,
) -> Output[PostgresTableIdentifier]:
    """The BAG Nummeraanduiding layer that only contains the current (timely) and
    physically existing objects. The data can be limited to a specific reference date by setting
    the *reference_date* parameter."""
    create_schema(computation_db, NEW_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(NEW_SCHEMA, "nummeraanduidingactueelbestaand")
    if config.reference_date is not None:
        reference_date = datetime.strptime(config.reference_date, "%Y-%m-%d").replace(
            tzinfo=UTC
        )
    else:
        reference_date = datetime.now(tz=datetime.now().astimezone().tzinfo)
    query = load_sql(
        query_params={
            "num_tbl": stage_bag_nummeraanduiding,
            "new_table": new_table,
            "reference_date": reference_date,
        }
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    return Output(new_table, metadata=metadata)
