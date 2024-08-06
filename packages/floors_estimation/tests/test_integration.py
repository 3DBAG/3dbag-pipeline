from bag3d.floors_estimation.code_location import defs
from bag3d.common.resources.files import file_store
from dagster import ExecuteInProcessResult


def test_job_floors_estimation(database, test_data_dir, input_data_dir, model_dir):
    resolved_job = defs.get_job_def("floors_estimation")

    resources={
            "db_connection": database,
            "file_store": file_store.configured(
                {"data_dir": str(test_data_dir), }),
            "file_store_fastssd": file_store.configured(
                {"data_dir": str(input_data_dir),}
            ),
            "model_store": model_dir,
        }
    result = resolved_job.execute_in_process(resources=resources)

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
