import pickle

import pytest
from bag3d.common.resources.files import file_store
from bag3d.party_walls import assets
from bag3d.party_walls.jobs import job_nl_party_walls
from dagster import (AssetKey, Definitions, ExecuteInProcessResult, IOManager,
                     SourceAsset, load_assets_from_package_module)


def mock_features_file_index(features_file_index, output_data_dir):
    class MockIOManager(IOManager):
        def load_input(self, context):
            return pickle.load(open(output_data_dir / "features_file_index.pkl", "rb"))

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["party_walls", features_file_index]),
        io_manager_def=MockIOManager(),
    )


def mock_distribution_tiles_files_index(
    distribution_tiles_files_index, output_data_dir
):
    class MockIOManager(IOManager):
        def load_input(self, context):
            return pickle.load(
                open(output_data_dir / "distribution_tiles_files_index.pkl", "rb")
            )

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["party_walls", distribution_tiles_files_index]),
        io_manager_def=MockIOManager(),
    )


@pytest.mark.slow
def test_job_party_walls(
    database, input_data_dir, export_dir_uncompressed, output_data_dir
):
    resources = {
        "db_connection": database,
        "file_store": file_store.configured(
            {
                "data_dir": str(export_dir_uncompressed),
            }
        ),
        "file_store_fastssd": file_store.configured(
            {
                "data_dir": str(input_data_dir),
            }
        ),
    }
    all_party_assets = load_assets_from_package_module(
        assets, key_prefix="party_walls", group_name="party_walls"
    )
    # Filter the assets to include only the ones we need
    party_assets = [
        asset
        for asset in all_party_assets
        if asset.key
        in {
            AssetKey(["party_walls", "cityjsonfeatures_with_party_walls_nl"]),
            AssetKey(["party_walls", "party_walls_nl"]),
        }
    ]

    defs = Definitions(
        resources=resources,
        assets=[
            mock_distribution_tiles_files_index(
                "distribution_tiles_files_index", output_data_dir
            ),
            mock_features_file_index("features_file_index", output_data_dir),
            party_assets[0],
            party_assets[1],
        ],
        jobs=[
            job_nl_party_walls,
        ],
    )

    resolved_job = defs.get_job_def("nl_party_walls")

    result = resolved_job.execute_in_process(
        resources=resources, partition_key="10/564/624"
    )

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
