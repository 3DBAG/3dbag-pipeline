from bag3d.common.utils.database import (create_schema, drop_table, load_sql,
                                         postgrestable_from_query,
                                         postgrestable_metadata, summary_md,
                                         table_exists)
from dagster import build_op_context
from pgutils import PostgresTableIdentifier
from psycopg.sql import SQL, Identifier

TEST_SCHEMA_NAME = "test"
TEST_TABLE_NAME = "table1"
EXISTING_TABLE = PostgresTableIdentifier("lvbag", "pandactueelbestaand")

# def test_load_sql():
#     query_params = {
#         'tbl': PostgresTableIdentifier('myschema', 'mytable'),
#     }
#     query = load_sql(filename="test_table2.sql", query_params=query_params)
#     expect = "Composed([SQL('create table table2 as select * from '), Identifier('myschema', 'mytable'), SQL(';')])"
#     assert str(query) == expect


def test_table_exists(database):
    context = build_op_context(resources={"db_connection": database})

    assert table_exists(context, EXISTING_TABLE) is True

    non_existing_table = PostgresTableIdentifier(TEST_SCHEMA_NAME, TEST_TABLE_NAME)
    assert table_exists(context, non_existing_table) is False


def test_drop_table(database):
    context = build_op_context(resources={"db_connection": database})
    create_schema(context, TEST_SCHEMA_NAME)
    tbl = PostgresTableIdentifier(TEST_SCHEMA_NAME, TEST_TABLE_NAME)
    query = SQL(
        """CREATE TABLE {table} (id INTEGER, value TEXT);
                   INSERT INTO {table} VALUES (1, 'bla');
                   INSERT INTO {table} VALUES (2, 'foo');"""
    ).format(table=Identifier(tbl.schema.str, tbl.table.str))

    metadata = postgrestable_from_query(context, query, tbl)
    assert metadata["Rows"] == 2

    assert table_exists(context, tbl) is True

    drop_table(context, tbl)

    assert table_exists(context, tbl) is False


def test_create_schema(database):
    context = build_op_context(resources={"db_connection": database})
    create_schema(context, TEST_SCHEMA_NAME)

    query = SQL(
        """SELECT count(schema_name)
                FROM information_schema.schemata 
                WHERE schema_name = {schema};"""
    ).format(schema=TEST_SCHEMA_NAME)
    res = context.resources.db_connection.get_dict(query)
    assert res[0]["count"] == 1


def test_summary_md(database):
    null_count = database.count_nulls(EXISTING_TABLE)
    fields = database.get_fields(EXISTING_TABLE)

    res = summary_md(fields, null_count)
    assert type(res) == str
    lines = res.splitlines()
    assert lines[0] == "| column | type | NULLs |"


def test_postgrestable_metadata(database):
    context = build_op_context(resources={"db_connection": database})
    table = PostgresTableIdentifier("lvbag", "pandactueelbestaand")

    res = postgrestable_metadata(context, table)

    print(res)
    assert (
        res["Database.Schema.Table"] == "baseregisters_test.lvbag.pandactueelbestaand"
    )
    assert res["Rows"] == 0


def test_postgrestable_from_query(database):
    context = build_op_context(resources={"db_connection": database})
    create_schema(context, TEST_SCHEMA_NAME)
    tbl = PostgresTableIdentifier(TEST_SCHEMA_NAME, TEST_TABLE_NAME)

    query = SQL(
        """CREATE TABLE {table} (id INTEGER, value TEXT);
                   INSERT INTO {table} VALUES (1, 'bla');
                   INSERT INTO {table} VALUES (2, 'foo');"""
    ).format(table=Identifier(tbl.schema.str, tbl.table.str))

    metadata = postgrestable_from_query(context, query, tbl)
    assert metadata["Rows"] == 2
    assert table_exists(context, tbl) is True

    drop_table(context, tbl)
    assert table_exists(context, tbl) is False
