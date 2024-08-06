from bag3d.party_walls.code_location import defs
from bag3d.common.resources.files import file_store
from dagster import ExecuteInProcessResult


def test_job_party_walls(input_data_dir, export_dir_uncompressed):
    resources={
            "file_store": file_store.configured(
                {"data_dir": str(export_dir_uncompressed), }),
            "file_store_fastssd": file_store.configured(
                {"data_dir": str(input_data_dir),}
            )
        }

    resolved_job = defs.get_job_def("nl_party_walls")

    result = resolved_job.execute_in_process(resources=resources, partition_key='10/564/624')

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success


