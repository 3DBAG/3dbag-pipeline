from dagster import asset, Output, get_dagster_logger, AutomationCondition

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import GDALResource
from bag3d.common.utils.database import (
    postgrestable_from_query,
    load_sql,
    drop_table,
    create_schema,
)
from bag3d.common.utils.geodata import ogr2postgres
from bag3d.common.types import PostgresTableIdentifier

SCHEMA_STAGE = "stage_bgt"
SCHEMA_PROD = "bgt"

logger = get_dagster_logger("bgt.load")


@asset(automation_condition=AutomationCondition.eager())
def stage_bgt_pand(
    db_connection: DatabaseResource, gdal: GDALResource, extract_bgt
) -> Output[PostgresTableIdentifier]:
    """The BGT Pand layer, loaded as-is from the extract."""
    create_schema(db_connection, SCHEMA_STAGE, logger=logger)
    xsd = "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd"
    new_table = PostgresTableIdentifier(SCHEMA_STAGE, "pand")
    # Need to explicitly drop the table just in case (...couz GDAL...)
    drop_table(db_connection, new_table, logger=logger)
    metadata = ogr2postgres(
        gdal_runner=gdal.runner,
        dataset="bgt",
        xsd=xsd,
        feature_type="pand",
        extract_path=extract_bgt,
        new_table=new_table,
        logger=logger,
        db_connection=db_connection,
    )
    return Output(new_table, metadata=metadata)


@asset(op_tags={"kind": "sql"}, automation_condition=AutomationCondition.eager())
def bgt_pandactueelbestaand(
    db_connection: DatabaseResource, stage_bgt_pand
) -> Output[PostgresTableIdentifier]:
    """The BGT Pand layer that only contains the current (timely) and physically
    existing objects, and repaired polygons."""
    create_schema(db_connection, SCHEMA_PROD, logger=logger)
    new_table = PostgresTableIdentifier(SCHEMA_PROD, "pandactueelbestaand")
    query = load_sql(query_params={"pand_tbl": stage_bgt_pand, "new_table": new_table})
    metadata = postgrestable_from_query(db_connection, query, new_table, logger=logger)
    return Output(new_table, metadata=metadata)
