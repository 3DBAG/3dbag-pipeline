import pytest
from bag3d.common.resources import file_store, gdal
from bag3d.core.assets.top10nl import download
from dagster import build_op_context


@pytest.mark.slow
def test_extract_top10nl(docker_gdal_image, wkt_testarea):
    """Does the complete asset work?"""
    context = build_op_context(
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": ["gebouw", ]
        },
        resources={
            "gdal": gdal.configured({"docker": {"image": docker_gdal_image}}),
            "file_store": file_store.configured({})
        }
    )
    res = download.extract_top10nl(context)
    assert res.value.exists()
    context.resources.file_store.rm(force=True)
