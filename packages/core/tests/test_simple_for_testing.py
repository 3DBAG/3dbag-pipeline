from bag3d.common.utils.database import (create_schema, drop_table,
                                         postgrestable_from_query,
                                         table_exists
                                         )
from dagster import build_op_context
from pgutils import PostgresTableIdentifier
from psycopg.sql import SQL, Identifier


def test_table_creation(database):
    context = build_op_context(
        resources={"db_connection": database}
    )
    create_schema(context, "test")
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

    bag_pandactueelbestaand = PostgresTableIdentifier('lvbag','pandactueelbestaand')

    assert table_exists(context, bag_pandactueelbestaand) is True

    drop_table(context, tbl)

    assert table_exists(context, tbl) is False
