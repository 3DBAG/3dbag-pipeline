from bag3d.common.utils.database import (create_schema, drop_table,
                                         postgrestable_from_query)
from dagster import build_op_context
from pgutils import PostgresTableIdentifier
from psycopg.sql import SQL, Identifier


def test_table_creation(database):
    context = build_op_context(
        resources={"db_connection": database}
    )
    create_schema(context, context.resources.db_connection, "test")
    tbl = PostgresTableIdentifier("test", "table1")
    query_params = {
        "table1": tbl,
    }

    query = SQL("""CREATE TABLE {table} (id INTEGER, value TEXT);
                   INSERT INTO {table} VALUES (1, 'bla');
                   INSERT INTO {table} VALUES (2, 'foo');""").format(
                       table = Identifier(tbl.schema.str, tbl.table.str))
        
    metadata = postgrestable_from_query(context, query, tbl)
    assert metadata["Rows"] == 2

    drop_table(context, context.resources.db_connection, tbl)

    query = SQL("""SELECT EXISTS (
                    SELECT FROM 
                        pg_tables
                    WHERE 
                        schemaname = '{schema}' AND 
                        tablename  = '{table}'
                    );""").format(schema=tbl.schema.id,
                                  table=tbl.table.id)
    res = context.resources.db_connection.get_dict(query)
    assert res[0]["exists"] == False
