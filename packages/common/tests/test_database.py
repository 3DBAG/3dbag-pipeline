from pgutils import PostgresTableIdentifier

from bag3d.common.utils.database import load_sql


def test_load_sql():
    query_params = {
        'tbl': PostgresTableIdentifier('myschema', 'mytable'),
    }
    query = load_sql(filename="test_table2.sql", query_params=query_params)
    expect = "Composed([SQL('create table table2 as select * from '), Identifier('myschema', 'mytable'), SQL(';')])"
    assert str(query) == expect
