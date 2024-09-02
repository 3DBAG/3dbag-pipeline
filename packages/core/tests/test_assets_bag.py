import pytest
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table, table_exists
from bag3d.core.assets.bag.download import (bagextract_metadata, extract_bag,
                                            load_bag_layer, stage_bag_layer)
from dagster import build_op_context


def test_get_extract_metadata(test_data_dir):
    init_context = build_op_context({})

    metadata = bagextract_metadata(init_context, test_data_dir / "lvbag-extract")
    assert metadata is not None
    assert metadata[0]["Gebied"] == "NLD"
    assert metadata[0]["Timeliness"] == "2022-10-08"
    assert metadata[1] == "08102022"


def test_load_bag_layer(context, test_data_dir):
    test_bag_table = PostgresTableIdentifier("lvbag", "test_ligplaats")
    res = load_bag_layer(
        context=context,
        extract_dir=test_data_dir / "lvbag-extract",
        layer="ligplaats",
        shortdate="08102022",
        new_table=test_bag_table,
    )
    assert res is True
    assert res is not None
    assert table_exists(context, test_bag_table) is True
    drop_table(context, test_bag_table)
    assert table_exists(context, test_bag_table) is False


@pytest.mark.slow
def test_extract_bag(context):
    res = extract_bag(context)
    assert res.value is not None


def test_stage_bag_layer(context, test_data_dir):
    res = stage_bag_layer(
        context,
        "ligplaats",
        "stage_lvbag",
        dict(),
        "08102022",
        test_data_dir / "lvbag-extract",
    )
    assert res is not None
    test_bag_table = PostgresTableIdentifier("stage_lvbag", "ligplaats")
    assert table_exists(context, test_bag_table) is True
    drop_table(context, test_bag_table)
    assert table_exists(context, test_bag_table) is False
