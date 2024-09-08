import pytest
from bag3d.common.resources.files import file_store
from bag3d.floors_estimation.code_location import defs
from dagster import ExecuteInProcessResult


@pytest.mark.slow
def test_job_floors_estimation(input_data_dir):
    resolved_job = defs.get_job_def("floors_estimation")

    resources = {
        "file_store_fastssd": file_store.configured(
            {
                "data_dir": str(input_data_dir),
            }
        )
    }
    result = resolved_job.execute_in_process(resources=resources)

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
