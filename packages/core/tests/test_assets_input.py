from dagster import build_op_context

from bag3d.core.assets.input import intermediary
from bag3d.common.types import PostgresTableIdentifier


def test_bag_kas_warenhuis(database):
    """Does the complete asset work?"""
    context = build_op_context(
        resources={"db_connection": database}
    )
    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
    top10nl_gebouw = PostgresTableIdentifier("top10nl", "gebouw")
    res = intermediary.bag_kas_warenhuis(context, bag_pandactueelbestaand,
                                         top10nl_gebouw)
