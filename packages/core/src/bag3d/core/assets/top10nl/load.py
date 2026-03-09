from dagster import asset, Output, get_dagster_logger, AutomationCondition
from psycopg.sql import SQL, Identifier

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import GDALResource
from bag3d.common.utils.database import (
    load_sql,
    postgrestable_from_query,
    drop_table,
    create_schema,
)
from bag3d.common.utils.geodata import ogr2postgres
from bag3d.common.types import PostgresTableIdentifier

logger = get_dagster_logger("top10nl.load")


@asset(automation_condition=AutomationCondition.eager())
def stage_top10nl_gebouw(
    computation_db: DatabaseResource, gdal: GDALResource, extract_top10nl
) -> Output[PostgresTableIdentifier]:
    """The TOP10NL Gebouw layer, loaded as-is from the extract."""
    new_schema = "stage_top10nl"
    create_schema(computation_db, new_schema, logger=logger)
    xsd = "https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd"
    new_table = PostgresTableIdentifier(new_schema, "gebouw")
    # Need to explicitly drop the table just in case (...couz GDAL...)
    drop_table(computation_db, new_table, logger=logger)
    metadata = ogr2postgres(
        gdal_runner=gdal.runner,
        computation_db=computation_db,
        dataset="top10nl",
        xsd=xsd,
        extract_path=extract_top10nl,
        feature_type="gebouw",
        new_table=new_table,
        logger=logger,
    )
    return Output(new_table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "sql"}, automation_condition=AutomationCondition.eager()
)
def top10nl_gebouw(
    computation_db: DatabaseResource, stage_top10nl_gebouw
) -> Output[PostgresTableIdentifier]:
    """The cleaned TOP10NL Gebouw polygon layer that only contains the current
    (timely) and physically existing buildings."""
    new_schema = "top10nl"
    create_schema(computation_db, new_schema, logger=logger)
    table_name = "gebouw"
    new_table = PostgresTableIdentifier("top10nl", table_name)
    query = load_sql(
        query_params={"gebouw_tbl": stage_top10nl_gebouw, "new_table": new_table}
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    computation_db.connection.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY (fid)").format(new_table.id)
    )
    geom_idx_name = f"{table_name}_geometrie_vlak_idx"
    computation_db.connection.send_query(
        SQL("CREATE INDEX {} ON {} USING gist (geometrie_vlak)").format(
            Identifier(geom_idx_name), new_table.id
        )
    )
    computation_db.connection.send_query(
        SQL("CREATE INDEX {} ON {} USING gin (typegebouw)").format(
            Identifier(f"{table_name}_typegebouw_idx"), new_table.id
        )
    )
    computation_db.connection.send_query(
        SQL("CLUSTER {} USING {}").format(new_table.id, Identifier(geom_idx_name))
    )
    return Output(new_table, metadata=metadata)
