from dagster import (asset, Output)

from bag3d.common.utils.database import (postgrestable_from_query, load_sql,
                                         drop_table, create_schema)
from bag3d.common.utils.geodata import ogr2postgres
from bag3d.common.types import PostgresTableIdentifier

SCHEMA_STAGE = "stage_bgt"
SCHEMA_PROD = "bgt"


@asset(required_resource_keys={"db_connection", "gdal"})
def stage_bgt_pand(context, extract_bgt) -> Output[PostgresTableIdentifier]:
    """The BGT Pand layer, loaded as-is from the extract."""
    create_schema(context, SCHEMA_STAGE)
    xsd = "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd"
    new_table = PostgresTableIdentifier(SCHEMA_STAGE, "pand")
    # Need to explicitly drop the table just in case (...couz GDAL...)
    drop_table(context, new_table)
    metadata = ogr2postgres(context=context, dataset="bgt", xsd=xsd,
                            feature_type="pand",
                            extract_path=extract_bgt, new_table=new_table)
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def bgt_pandactueelbestaand(context, stage_bgt_pand) -> Output[PostgresTableIdentifier]:
    """The BGT Pand layer that only contains the current (timely) and physically
    existing objects, and repaired polygons."""
    create_schema(context, SCHEMA_PROD)
    new_table = PostgresTableIdentifier(SCHEMA_PROD, "pandactueelbestaand")
    query = load_sql(query_params={"pand_tbl": stage_bgt_pand,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection", "gdal"})
def stage_bgt_wegdeel(context, extract_bgt) -> Output[PostgresTableIdentifier]:
    """The BGT Wegdeel layer, loaded as-is from the extract."""
    create_schema(context, SCHEMA_STAGE)
    xsd = "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd"
    new_table = PostgresTableIdentifier(SCHEMA_STAGE, "wegdeel")
    # Need to explicitly drop the table just in case (...couz GDAL...)
    drop_table(context, new_table)
    metadata = ogr2postgres(context=context, dataset="bgt", xsd=xsd,
                            new_table=new_table,
                            extract_path=extract_bgt, feature_type="wegdeel")
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def bgt_wegdeelactueelbestaand(context, stage_bgt_wegdeel
                               ) -> Output[PostgresTableIdentifier]:
    """The BGT Wegdeel layer that only contains the current (timely) and physically
    existing objects, and repaired polygons."""
    create_schema(context, SCHEMA_PROD)
    new_table = PostgresTableIdentifier(SCHEMA_PROD, "wegdeelactueelbestaand")
    query = load_sql(query_params={"wegdeel_tbl": stage_bgt_wegdeel,
                                   "new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)
