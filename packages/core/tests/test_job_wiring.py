import dagster as dg

from bag3d.common.testing.mock_resources import MOCK_RESOURCES
from bag3d.core.code_location import all_assets, all_sensors, defs
from bag3d.core.jobs import (
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn6,
    job_ahn_metadata_index,
    job_ahn_tile_index,
    job_bgt,
    job_reconstruct,
    job_reconstruct_debug,
    job_source_input,
)

JOB_DEFS = [
    job_bgt,
    job_source_input,
    job_ahn_tile_index,
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn6,
    job_ahn_metadata_index,
    job_reconstruct,
    job_reconstruct_debug,
]


def test_all_jobs_resolvable():
    for job_def in JOB_DEFS:
        assert defs.get_job_def(job_def.name) is not None


def test_partitioned_jobs_have_partition_defs():
    for job_name in ("ahn3", "ahn4", "ahn5", "ahn6"):
        assert defs.get_job_def(job_name).partitions_def is not None


def test_core_assets_validate_with_mock_resources():
    mocked_defs = dg.Definitions(
        assets=all_assets,
        jobs=JOB_DEFS,
        resources=MOCK_RESOURCES,
        sensors=all_sensors,
    )
    dg.Definitions.validate_loadable(mocked_defs)
