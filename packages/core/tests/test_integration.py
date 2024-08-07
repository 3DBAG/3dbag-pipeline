import pickle

import pytest
from bag3d.common.resources import gdal
from bag3d.common.resources.executables import geoflow, roofer
from bag3d.common.resources.files import file_store
from bag3d.common.resources.temp_until_configurableresource import (
    EXE_PATH_GEOF, EXE_PATH_ROOFER_CROP, FLOWCHART_PATH_RECONSTRUCT)
from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets import reconstruction
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA
from bag3d.core.jobs import job_nl_reconstruct
from dagster import (AssetKey, Definitions, ExecuteInProcessResult, IOManager,
                     Output, SourceAsset, load_assets_from_package_module)


def mock_reconstruction_input(reconstruction_input):
    class MockIOManager(IOManager):
        def load_input(self, context):
            new_table = PostgresTableIdentifier(
                RECONSTRUCTION_INPUT_SCHEMA, "reconstruction_input"
            )
            return new_table

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["input", reconstruction_input]),
        io_manager_def=MockIOManager(),
    )


def mock_tiles(tiles):
    class MockIOManager(IOManager):
        def load_input(self, context):
            new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "tiles")
            return new_table

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["input", tiles]),
        io_manager_def=MockIOManager(),
    )


def mock_index(index):
    class MockIOManager(IOManager):
        def load_input(self, context):
            new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "index")
            return new_table

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["input", index]),
        io_manager_def=MockIOManager(),
    )


def mock_regular_grid_200m(regular_grid_200m):
    class MockIOManager(IOManager):
        def load_input(self, context):
            new_table = PostgresTableIdentifier("ahn", "regular_grid_200m")
            return new_table

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["ahn", regular_grid_200m]),
        io_manager_def=MockIOManager(),
    )


@pytest.mark.slow
def test_job_nl_reconstruct(database, docker_gdal_image, test_data_dir):
    resources = {
        "geoflow": geoflow.configured(
            {
                "exes": {"geof": EXE_PATH_GEOF},
                "flowcharts": {"reconstruct": FLOWCHART_PATH_RECONSTRUCT},
            }
        ),
        "roofer": roofer.configured(
            {
                "exes": {"crop": EXE_PATH_ROOFER_CROP},
            }
        ),
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

    all_reconstruction_assets = load_assets_from_package_module(
        reconstruction, key_prefix="reconstruction", group_name="reconstruction"
    )

    # Filter the assets to include only the ones we need
    reconstruction_assets = [
        asset
        for asset in all_reconstruction_assets
        if asset.key
        in {
            AssetKey(["reconstruction", "cropped_input_and_config_nl"]),
            AssetKey(["reconstruction", "reconstructed_building_models_nl"]),
        }
    ]

    defs = Definitions(
        resources=resources,
        assets=[
            mock_regular_grid_200m("regular_grid_200m"),
            mock_reconstruction_input("reconstruction_input"),
            mock_tiles("tiles"),
            mock_index("index"),
            reconstruction_assets[0],
            reconstruction_assets[1],
        ],
        jobs=[
            job_nl_reconstruct,
        ],
    )

    resolved_job = defs.get_job_def("nl_reconstruct")
    result = resolved_job.execute_in_process(
        resources=resources, partition_key="10/564/624"
    )

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
