import pytest
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.party_walls import assets
from bag3d.party_walls.jobs import job_nl_party_walls, job_nl_party_walls_index
from dagster import (
    Definitions,
    ExecuteInProcessResult,
    load_assets_from_package_module,
    DagsterInstance,
)


@pytest.mark.needs_tools
def test_job_party_walls(
    database,
    party_walls_file_store,
    party_walls_file_store_fastssd,
):
    resources = {
        "db_connection": database,
        "file_store": FileStoreResource(data_dir=str(party_walls_file_store)),
        "file_store_fastssd": FileStoreResource(
            data_dir=str(party_walls_file_store_fastssd)
        ),
        "version": ReleaseVersionResource("test_version"),
    }
    all_party_assets = load_assets_from_package_module(
        assets, key_prefix="party_walls", group_name="party_walls"
    )

    defs = Definitions(
        resources=resources,
        assets=all_party_assets,
        jobs=[
            job_nl_party_walls_index,
            job_nl_party_walls,
        ],
    )

    with DagsterInstance.ephemeral() as instance:
        resolved_job = defs.get_job_def("nl_party_walls_index")
        result = resolved_job.execute_in_process(instance=instance, resources=resources)
        assert isinstance(result, ExecuteInProcessResult)
        assert result.success

        resolved_job = defs.get_job_def("nl_party_walls")
        result = resolved_job.execute_in_process(
            instance=instance, resources=resources, partition_key="0/0/0"
        )
        assert isinstance(result, ExecuteInProcessResult)
        assert result.success
