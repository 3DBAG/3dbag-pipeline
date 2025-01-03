import pytest
from bag3d.common.utils.database import (
    create_schema,
    drop_table,
    load_sql,
    postgrestable_from_query,
    postgrestable_metadata,
    summary_md,
    table_exists,
)
from pgutils import PostgresTableIdentifier
from psycopg.sql import SQL, Identifier

TEST_SCHEMA_NAME = "test"
EXISTING_TABLE = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
NON_EXISTING_TABLE = PostgresTableIdentifier("public", "non_existing_table")


def test_table_exists(context):
    assert table_exists(context, EXISTING_TABLE) is True
    assert table_exists(context, NON_EXISTING_TABLE) is False


def test_drop_table(context):
    query = SQL(
        """CREATE TABLE IF NOT EXISTS  {table} (id INTEGER, value TEXT);
                   INSERT INTO {table} VALUES (1, 'bla');
                   INSERT INTO {table} VALUES (2, 'foo');"""
    ).format(
        table=Identifier(NON_EXISTING_TABLE.schema.str, NON_EXISTING_TABLE.table.str)
    )
    context.resources.db_connection.connect.send_query(query)
    assert table_exists(context, NON_EXISTING_TABLE) is True
    drop_table(context, NON_EXISTING_TABLE)
    assert table_exists(context, NON_EXISTING_TABLE) is False


def test_create_schema(context):
    create_schema(context, TEST_SCHEMA_NAME)

    query = SQL(
        """SELECT count(schema_name)
                FROM information_schema.schemata 
                WHERE schema_name = {schema};"""
    ).format(schema=TEST_SCHEMA_NAME)
    res = context.resources.db_connection.connect.get_dict(query)
    assert res[0]["count"] == 1


def test_summary_md(database):
    null_count = database.connect.count_nulls(EXISTING_TABLE)
    fields = database.connect.get_fields(EXISTING_TABLE)

    res = summary_md(fields, null_count)
    assert isinstance(res, str)
    lines = res.splitlines()
    assert lines[0] == "| column | type | NULLs |"


def test_postgrestable_metadata(context):
    res = postgrestable_metadata(context, EXISTING_TABLE)

    assert (
        res["Database.Schema.Table"] == "baseregisters_test.lvbag.pandactueelbestaand"
    )
    assert res["Rows"] == 414


def test_postgrestable_from_query(context):
    create_schema(context, TEST_SCHEMA_NAME)
    tbl = PostgresTableIdentifier("public", "test_table")
    assert table_exists(context, tbl) is False

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


@pytest.mark.skip(reason="Cannot find module.")
def test_load_sql():  # pragma: no cover
    query_params = {
        "tbl": PostgresTableIdentifier("myschema", "mytable"),
    }
    query = load_sql(filename="test_table2.sql", query_params=query_params)
    expect = "Composed([SQL('create table table2 as select * from '), Identifier('myschema', 'mytable'), SQL(';')])"
    assert str(query) == expect
