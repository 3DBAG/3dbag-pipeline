from dagster import asset, Output

from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import (create_schema,
                                         load_sql,
                                         postgrestable_from_query)

SCHEMA = "floors_estimation"


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def extract_external_features(context):
    """Creates the `floors_estimation.building_features_external` table."""
    create_schema(context, context.resources.db_connection, SCHEMA)
    table_name = "building_features_external_test"
    new_table = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(query_params={"new_table": new_table,
                                   "table_name": table_name})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def create_building_features_table(context):
    """Creates the `floors_estimation.building_features` table."""
    create_schema(context, context.resources.db_connection, SCHEMA)
    table_name = "building_features_test"
    new_table = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(query_params={"new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)
