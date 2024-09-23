import os

import pytest
from bag3d.common.resources.executables import (
    GeoflowResource,
    DOCKER_GDAL_IMAGE,
    GDALResource,
    TylerResource,
    RooferResource,
    DockerConfig,
)
from bag3d.common.resources.files import file_store
from bag3d.core.assets import export, reconstruction
from bag3d.core.jobs import job_nl_export, job_nl_reconstruct
from dagster import (
    AssetKey,
    Definitions,
    ExecuteInProcessResult,
    load_assets_from_package_module,
)


@pytest.mark.integration
def test_integration_reconstruction_and_export(
    database,
    test_data_dir,
    mock_asset_regular_grid_200m,
    mock_asset_reconstruction_input,
    mock_asset_tiles,
    mock_asset_index,
):
    # update quadtree
    og_quadtree = test_data_dir / "quadtree.tsv"
    export_dir = test_data_dir / "reconstruction_input" / "3DBAG" / "export"
    os.system(f"cp {og_quadtree} {export_dir}")

    resources = {
        "tyler": TylerResource(
            exe_tyler=os.getenv("EXE_PATH_TYLER"),
            exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"),
        ).app,
        "geoflow": GeoflowResource(
            exe_geoflow=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
            flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"),
        ).app,
        "roofer": RooferResource(exe_roofer_crop=os.getenv("EXE_PATH_ROOFER_CROP")).app,
        "gdal": GDALResource(
            exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
            exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
            exe_sozip=os.getenv("EXE_PATH_SOZIP"),
        ).app,
        "db_connection": database,
        "file_store": file_store.configured(
            {
                "data_dir": str(test_data_dir / "reconstruction_input"),
            }
        ),
        "file_store_fastssd": file_store.configured(
            {
                "data_dir": str(test_data_dir / "integration_core"),
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
