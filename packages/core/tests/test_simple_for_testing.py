"""
These are tests that are used for developing and debugging the things in the
'simple_for_testing' package. The 'simple_for_testing' has very minimal things that
run quickly without big dependencies, and help in developing some new concept,
workflow, logic etc.
"""
from dagster import build_op_context
from bag3d.core.simple_for_testing import test_table1, test_table2


def test_tables(resource_db_connection_docker, resource_container):
    context = build_op_context(
        resources={"container": resource_container,
                   "db_connection": resource_db_connection_docker}
    )
    res1 = test_table1(context)
    res2 = test_table2(context, res1)
