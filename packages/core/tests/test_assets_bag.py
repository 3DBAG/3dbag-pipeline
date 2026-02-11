from dagster import build_op_context

from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table, table_exists
from bag3d.core.assets.bag.download import (
    bagextract_metadata,
    load_bag_layer,
    stage_bag_layer,
)


def test_get_extract_metadata(test_data_dir):
    init_context = build_op_context({})

    metadata = bagextract_metadata(init_context, test_data_dir / "lvbag-extract")
    assert metadata is not None
    assert metadata[0]["Gebied"] == "NLD"
    assert metadata[0]["Timeliness"] == "2022-10-08"
    assert metadata[1] == "08102022"


def test_load_bag_layer(database, file_store, gdal, test_data_dir):
    # Build context directly for non-asset function calls
    from bag3d.common.resources.version import VersionResource

    test_bag_table = PostgresTableIdentifier("lvbag", "test_ligplaats")

    build_op_context(
        partition_key="01cz1",
        resources={
            "gdal": gdal,
            "db_connection": database,
            "file_store": file_store,
            "version": VersionResource("test_version"),
        },
    )

    res = load_bag_layer(
        db_connection=database,
        gdal=gdal,
        extract_dir=test_data_dir / "lvbag-extract",
        layer="ligplaats",
        shortdate="08102022",
        new_table=test_bag_table,
        remove_zip=False,
        with_parallel=False,
        geofilter=None,
    )
    assert res is True
    assert res is not None
    assert table_exists(database, test_bag_table) is True
    drop_table(database, test_bag_table)
    assert table_exists(database, test_bag_table) is False


def test_stage_bag_layer(database, file_store, gdal, test_data_dir):
    # Build context directly for non-asset function calls
    from bag3d.common.resources.version import VersionResource

    context = build_op_context(
        partition_key="01cz1",
        resources={
            "gdal": gdal,
            "db_connection": database,
            "file_store": file_store,
            "version": VersionResource("test_version"),
        },
    )

    res = stage_bag_layer(
        db_connection=database,
        gdal=gdal,
        layer="ligplaats",
        new_schema="stage_lvbag",
        metadata=dict(),
        shortdate="08102022",
        extract_dir=test_data_dir / "lvbag-extract",
        remove_zip=False,
        with_parallel=False,
        geofilter=None,
        context=context,
    )
    assert res is not None
    test_bag_table = PostgresTableIdentifier("stage_lvbag", "ligplaats")
    assert table_exists(database, test_bag_table) is True
    drop_table(database, test_bag_table)
    assert table_exists(database, test_bag_table) is False
