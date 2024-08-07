from bag3d.core.code_location import defs
from bag3d.common.resources import gdal
from bag3d.common.resources.files import file_store
from dagster import ExecuteInProcessResult


def test_job_nl_reconstruct(database, docker_gdal_image, test_data_dir):
    resolved_job = defs.get_job_def("nl_reconstruct")
    print(resolved_job)
    resources = {
        "gdal": gdal.configured({"docker": {"image": docker_gdal_image}}),
        "db_connection": database,
        "file_store": file_store.configured(
            {
                "data_dir": str(test_data_dir),
            }
        ),
        "file_store_fastssd": file_store.configured(
            {
                "data_dir": str(test_data_dir / "reconstruction_data" / "input"),
            }
        ),
    }
    result = resolved_job.execute_in_process(resources=resources)

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
