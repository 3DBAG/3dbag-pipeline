import pytest
from dagster import Definitions, DagsterInstance, AssetKey

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
    )

    with DagsterInstance.ephemeral() as instance:
        job = defs.get_implicit_global_asset_job_def()
        result = job.execute_in_process(
            instance=instance,
            asset_selection=[AssetKey(["bgt", "extract_bgt"])],
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
