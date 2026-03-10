import pytest
from psycopg.sql import SQL

from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table
from bag3d.core.assets.input import intermediary, tile
from dagster import get_dagster_logger, Output


def test_bag_kas_warenhuis(database):
    """Does the bag_kas_warenhuis asset work?"""
    logger = get_dagster_logger()
    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
    top10nl_gebouw = PostgresTableIdentifier("top10nl", "gebouw")

    new_table = PostgresTableIdentifier("reconstruction_input", "bag_kas_warenhuis")

    res = intermediary.bag_kas_warenhuis(
        bag_pandactueelbestaand,
        top10nl_gebouw,
        database,
    )
    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert str(res.value) == f"{new_table.schema}.{new_table.table}"
    drop_table(database, new_table, logger)


def test_bag_bag_overlap(database):
    """Does the bag_bag_overlap asset work?"""
    logger = get_dagster_logger()
    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")

    new_table = PostgresTableIdentifier("reconstruction_input", "bag_bag_overlap")

    res = intermediary.bag_bag_overlap(
        bag_pandactueelbestaand,
        database,
    )
    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert str(res.value) == f"{new_table.schema}.{new_table.table}"
    drop_table(database, new_table, logger)


def test_bag_pand_vbo_views(database):
    """Does the bag_pand_vbo_views asset work?"""
    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
    bag_verblijfsobjectactueelbestaand = PostgresTableIdentifier(
        "lvbag", "verblijfsobjectactueelbestaand"
    )

    new_table = PostgresTableIdentifier("reconstruction_input", "pand_vbo_single")

    res = intermediary.bag_pand_vbo_views(
        bag_pandactueelbestaand,
        bag_verblijfsobjectactueelbestaand,
        database,
    )
    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert str(res.value) == f"{new_table.schema}.{new_table.table}"
    # cleanup views
    for view in ("pand_vbo_single", "pand_vbo_multi", "pand_vbo_woonfunctie"):
        database.connection.send_query(
            SQL("DROP VIEW IF EXISTS reconstruction_input.{} CASCADE").format(SQL(view))
        )


@pytest.mark.slow
def test_bag_adjacency(database):
    """Does the bag_adjacency asset work?"""
    logger = get_dagster_logger()
    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")

    new_table = PostgresTableIdentifier("reconstruction_input", "bag_adjacency")

    res = intermediary.bag_adjacency(
        bag_pandactueelbestaand,
        database,
    )
    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert str(res.value) == f"{new_table.schema}.{new_table.table}"
    drop_table(database, new_table, logger)


@pytest.mark.slow
def test_bag_building_type(database):
    """Does the bag_building_type asset work?"""
    logger = get_dagster_logger()
    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
    bag_verblijfsobjectactueelbestaand = PostgresTableIdentifier(
        "lvbag", "verblijfsobjectactueelbestaand"
    )
    # Create the views first as upstream dependency
    intermediary.bag_pand_vbo_views(
        bag_pandactueelbestaand,
        bag_verblijfsobjectactueelbestaand,
        database,
    )
    pand_vbo_views_result = PostgresTableIdentifier(
        "reconstruction_input", "pand_vbo_single"
    )

    new_table = PostgresTableIdentifier("reconstruction_input", "woningtypen")

    res = intermediary.bag_building_type(
        bag_pandactueelbestaand,
        pand_vbo_views_result,
        database,
    )
    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    assert str(res.value) == f"{new_table.schema}.{new_table.table}"
    drop_table(database, new_table, logger)
    # cleanup views
    for view in ("pand_vbo_single", "pand_vbo_multi", "pand_vbo_woonfunctie"):
        database.connection.send_query(
            SQL("DROP VIEW IF EXISTS reconstruction_input.{} CASCADE").format(SQL(view))
        )


def test_get_tile_ids():
    """Does the get_tile_ids produce tile_ids?"""
    schema = "reconstruction_input"
    table_tiles = "tiles"
    logger = get_dagster_logger()
    res = tile.get_tile_ids(schema, table_tiles, logger=logger)
    assert isinstance(res, list)
    assert "10/564/626" in res
