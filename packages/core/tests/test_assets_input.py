from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table
from bag3d.core.assets.input import intermediary


def test_bag_kas_warenhuis(baseregisters_context):
    """Does the bag_kas_warenhuis asset work?"""

    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
    top10nl_gebouw = PostgresTableIdentifier("top10nl", "gebouw")

    new_table = PostgresTableIdentifier('reconstruction_input', "bag_kas_warenhuis")

    res = intermediary.bag_kas_warenhuis(baseregisters_context, bag_pandactueelbestaand,
                                         top10nl_gebouw)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert  str(res.value) == f'{new_table.schema}.{new_table.table}'
    drop_table(baseregisters_context, new_table)

def test_bag_bag_overlap(baseregisters_context):
    """Does the bag_bag_overlap asset work?"""

    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")

    new_table = PostgresTableIdentifier('reconstruction_input', "bag_bag_overlap")

    res = intermediary.bag_bag_overlap(baseregisters_context, bag_pandactueelbestaand)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert  str(res.value) == f'{new_table.schema}.{new_table.table}'
    drop_table(baseregisters_context, new_table)
