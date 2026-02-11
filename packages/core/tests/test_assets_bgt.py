import pytest
from dagster import Definitions, DagsterInstance, define_asset_job

from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.core.assets.bgt.download import extract_bgt


@pytest.mark.slow
def test_extract_bgt(database, file_store, gdal, wkt_testarea):
    """Does the complete asset work?"""
    resources = {
        "gdal": gdal,
        "file_store": file_store,
        "db_connection": database,
        "version": ReleaseVersionResource("test_version"),
    }

    defs = Definitions(
        resources=resources,
        assets=[extract_bgt],
        jobs=[define_asset_job("test_bgt", selection=[extract_bgt])],
    )

    with DagsterInstance.ephemeral() as instance:
        result = defs.get_job_def("test_bgt").execute_in_process(
            instance=instance,
            run_config={
                "ops": {
                    "extract_bgt": {
                        "config": {
                            "featuretypes": ["pand"],
                            "geofilter": wkt_testarea,
                        }
                    }
                }
            },
        )
        assert result.success
        output = result.output_for_node("extract_bgt")
        assert output.exists()
