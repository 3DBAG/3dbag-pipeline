from bag3d.party_walls.jobs import job_nl_party_walls
from bag3d.common.resources.files import file_store
from dagster import ExecuteInProcessResult, asset, Definitions
import pickle
from pathlib import Path
from bag3d.party_walls.assets.party_walls import TilesFilesIndex, party_walls_nl, cityjsonfeatures_with_party_walls_nl



@asset(name="features_file_index")
def mock_features_file_index(output_data_dir):
    return pickle.load(open(output_data_dir / "features_file_index.pkl", "rb"))


@asset(name="distribution_tiles_files_index")
def mock_distribution_tiles_files_index(output_data_dir):
    return pickle.load(open(output_data_dir / "distribution_tiles_files_index.pkl", "rb"))


def test_job_party_walls(database, input_data_dir, export_dir_uncompressed, output_data_dir):
    resources={
            "db_connection": database,
            "file_store": file_store.configured(
                {"data_dir": str(export_dir_uncompressed), }),
            "file_store_fastssd": file_store.configured(
                {"data_dir": str(input_data_dir),}
            )
        }
    defs = Definitions(
    resources=resources,
    assets=[mock_distribution_tiles_files_index(output_data_dir),
            mock_features_file_index(output_data_dir),
            party_walls_nl,
            cityjsonfeatures_with_party_walls_nl
            ],
    jobs=[job_nl_party_walls, ]
    )


    resolved_job = defs.get_job_def("nl_party_walls")

    result = resolved_job.execute_in_process(resources=resources, partition_key='10/564/624')

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success


