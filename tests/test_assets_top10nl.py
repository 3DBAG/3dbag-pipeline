from dagster import build_op_context

from bag3d_pipeline.assets.top10nl import download
from bag3d_pipeline.resources import gdal, file_store


def test_extract_top10nl(docker_gdal_image, wkt_testarea):
    """Does the complete asset work?"""
    context = build_op_context(
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": ["gebouw",]
        },
        resources={
            "gdal": gdal.configured({"docker": {"image": docker_gdal_image}}),
            "file_store": file_store.configured({})
        }
    )
    res = download.extract_top10nl(context)
    assert res.value.exists()
    context.resources.file_store.rm(force=True)
