from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table
from bag3d.core.assets.input import intermediary, tile
from dagster import get_dagster_logger


def test_bag_kas_warenhuis(context):
    """Does the bag_kas_warenhuis asset work?"""

    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
    top10nl_gebouw = PostgresTableIdentifier("top10nl", "gebouw")

    new_table = PostgresTableIdentifier("reconstruction_input", "bag_kas_warenhuis")

    res = intermediary.bag_kas_warenhuis(
        context, bag_pandactueelbestaand, top10nl_gebouw
    )
    assert isinstance(res.value, PostgresTableIdentifier)
    assert str(res.value) == f"{new_table.schema}.{new_table.table}"
    drop_table(context, new_table)


def test_bag_bag_overlap(context):
    """Does the bag_bag_overlap asset work?"""

    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")

    new_table = PostgresTableIdentifier("reconstruction_input", "bag_bag_overlap")

    res = intermediary.bag_bag_overlap(context, bag_pandactueelbestaand)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert str(res.value) == f"{new_table.schema}.{new_table.table}"
    drop_table(context, new_table)


def test_get_tile_ids():
    """Does the get_tile_ids produce tile_ids?"""
    schema = "reconstruction_input"
    table_tiles = "tiles"
    logger = get_dagster_logger()
    res = tile.get_tile_ids(schema, table_tiles, logger=logger)
    assert isinstance(res, list)
    assert "10/564/626" in res
