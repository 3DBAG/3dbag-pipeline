import pytest
from dagster import Definitions, DagsterInstance, define_asset_job

from bag3d.common.resources.version import VersionResource
from bag3d.core.assets.top10nl.download import extract_top10nl


@pytest.mark.slow
def test_extract_top10nl(database, file_store, gdal, wkt_testarea):
    """Does the complete asset work?"""
    resources = {
        "gdal": gdal,
        "file_store": file_store,
        "db_connection": database,
        "version": VersionResource("test_version"),
    }

    defs = Definitions(
        resources=resources,
        assets=[extract_top10nl],
        jobs=[define_asset_job("test_top10nl", selection=[extract_top10nl])],
    )

    with DagsterInstance.ephemeral() as instance:
        result = defs.get_job_def("test_top10nl").execute_in_process(
            instance=instance,
            run_config={
                "ops": {
                    "extract_top10nl": {
                        "config": {
                            "featuretypes": ["gebouw"],
                            "geofilter": wkt_testarea,
                        }
                    }
                }
            },
        )
        assert result.success
        output = result.output_for_node("extract_top10nl")
        assert output.exists()
