import pytest
from bag3d.common.resources.files import FileStoreResource
from bag3d.floors_estimation.code_location import defs
from dagster import ExecuteInProcessResult


@pytest.mark.needs_tools
def test_job_floors_estimation(
    floors_estimation_file_store_fastssd, resources
):
    resolved_job = defs.get_job_def("floors_estimation")

    # Use resources fixture which has all necessary resources configured
    result = resolved_job.execute_in_process(
        resources={
            **resources,
            "file_store_fastssd": FileStoreResource(
                data_dir=str(floors_estimation_file_store_fastssd)
            ),
        }
    )

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
