from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table
from bag3d.core.assets.input import intermediary
from dagster import build_op_context


def test_bag_kas_warenhuis(database):
    """Does the complete asset work?"""
    context = build_op_context(
        resources={"db_connection": database}
    )
    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
    top10nl_gebouw = PostgresTableIdentifier("top10nl", "gebouw")

    new_table = PostgresTableIdentifier('reconstruction_input', "bag_kas_warenhuis")

    res = intermediary.bag_kas_warenhuis(context, bag_pandactueelbestaand,
                                         top10nl_gebouw)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert  str(res.value) == f'{new_table.schema}.{new_table.table}'
    drop_table(context, new_table)
