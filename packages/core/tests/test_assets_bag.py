import pytest
from bag3d.common.resources import gdal
from bag3d.common.resources.files import file_store
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table, table_exists
from bag3d.core.assets.bag.download import (bagextract_metadata, extract_bag,
                                            load_bag_layer)
from dagster import build_op_context


def test_get_extract_metadata(test_data_dir):
    init_context = build_op_context({})

    metadata = bagextract_metadata(init_context,
                                   test_data_dir / "lvbag-extract"
                                   )
    assert metadata is not None
    assert metadata[0]["Gebied"] == "NLD"
    assert metadata[0]["Timeliness"] == "2022-10-08"
    assert metadata[1] == "08102022"


def test_load_bag_layer(database, test_data_dir, wkt_testarea, docker_gdal_image):
    context = build_op_context( 
            op_config={
            "geofilter": wkt_testarea,
            "featuretypes": ["gebouw", ]
        },
            resources={
                    "db_connection": database,
                   "gdal": gdal.configured({"docker": {"image": docker_gdal_image}})
        }
)
    test_bag_table = PostgresTableIdentifier("lvbag", "test_ligplaats")
    res = load_bag_layer(context = context,
                   extract_dir = test_data_dir / "lvbag-extract",
                   layer="ligplaats", 
                   shortdate ="08102022",
                   new_table=test_bag_table)
    assert res is True
    assert res is not None
    assert table_exists(context, test_bag_table) is True
    drop_table(context, test_bag_table)
    assert table_exists(context, test_bag_table) is False


@pytest.mark.slow
def test_extract_bag(docker_gdal_image, wkt_testarea, temp_file_store ):
    context = build_op_context(
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": ["gebouw", ]
        },
        resources={
            "gdal": gdal.configured({"docker": {"image": docker_gdal_image}}),
            "file_store": file_store.configured(
                {"data_dir": str(temp_file_store), })
        }
    )

    res = extract_bag(context)
    print(str(temp_file_store))
    print(res.metadata)
    assert res.value is not None