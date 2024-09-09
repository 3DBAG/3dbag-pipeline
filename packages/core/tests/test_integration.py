import os

import pytest
from bag3d.common.resources.executables import geoflow, roofer, tyler, gdal, DOCKER_GDAL_IMAGE
from bag3d.common.resources.files import file_store
from bag3d.core.assets import export, reconstruction
from bag3d.core.jobs import job_nl_export, job_nl_reconstruct
from dagster import (
    AssetKey,
    Definitions,
    ExecuteInProcessResult,
    load_assets_from_package_module,
)


@pytest.mark.slow
def test_integration_reconstruction_and_export(
    database,
    docker_gdal_image,
    test_data_dir,
    mock_asset_regular_grid_200m,
    mock_asset_reconstruction_input,
    mock_asset_tiles,
    mock_asset_index,
):
    resources = {
        "tyler": tyler.configured(
            {
                "exes": {
                    "tyler-db": os.getenv("EXE_PATH_TYLER_DB"),
                    "tyler": os.getenv("EXE_PATH_TYLER"),
                }
            }
        ),
        "geoflow": geoflow.configured(
            {
                "exes": {"geof": os.getenv("EXE_PATH_ROOFER_RECONSTRUCT")},
                "flowcharts": {"reconstruct": os.getenv("FLOWCHART_PATH_RECONSTRUCT")},
            }
        ),
        "roofer": roofer.configured(
            {
                "exes": {"crop": os.getenv("EXE_PATH_ROOFER_CROP")},
            }
        ),
        "gdal": gdal.configured(
            {
                "docker": {
                    "image": DOCKER_GDAL_IMAGE,
                    "mount_point": "/tmp"
                }
            }
        ),
        "db_connection": database,
        "file_store": file_store.configured(
            {
                "data_dir": str(test_data_dir / "reconstruction_data" / "input"),
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

    all_export_assets = load_assets_from_package_module(
        export, key_prefix="export", group_name="export"
    )

    defs = Definitions(
        resources=resources,
        assets=[
            mock_asset_regular_grid_200m,
            mock_asset_reconstruction_input,
            mock_asset_tiles,
            mock_asset_index,
            *reconstruction_assets,
            *all_export_assets,
        ],
        jobs=[
            job_nl_reconstruct,
            job_nl_export,
        ],
    )

    resolved_job = defs.get_job_def("nl_reconstruct")
    result = resolved_job.execute_in_process(
        resources=resources, partition_key="10/564/624"
    )

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success

    resolved_job = defs.get_job_def("nl_export")
    result = resolved_job.execute_in_process(resources=resources)

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
