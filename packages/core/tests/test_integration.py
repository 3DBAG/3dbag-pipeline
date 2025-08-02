import os

import pytest

from bag3d.common.resources import Specs3DBAGResource
from bag3d.common.resources.executables import (
    GeoflowResource,
    GDALResource,
    TylerResource,
    RooferResource,
    ValidationResource,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import VersionResource
from bag3d.core.assets import export, reconstruction, deploy, release
from bag3d.core.jobs import (
    job_nl_export,
    job_nl_export_after_floors,
    job_nl_reconstruct,
    job_nl_deploy,
    job_nl_release,
)
from dagster import (
    AssetKey,
    Definitions,
    ExecuteInProcessResult,
    load_assets_from_package_module,
    DagsterInstance,
)


@pytest.mark.needs_tools
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
    export_dir = (
        test_data_dir / "reconstruction_input" / "3DBAG" / "export_test_version"
    )
    export_dir.mkdir(exist_ok=True)
    os.system(f"cp {og_quadtree} {export_dir}")

    resources = {
        "tyler": TylerResource(
            exe_tyler=os.getenv("EXE_PATH_TYLER"),
            exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"),
            exe_tyler_multiformat=os.getenv("EXE_PATH_TYLER_MULTIFORMAT"),
        ),
        "geoflow": GeoflowResource(
            exe_geoflow=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
            flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"),
        ),
        "roofer": RooferResource(
            exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"),
            exe_crop=os.getenv("EXE_PATH_ROOFER_CROP"),
        ),
        "gdal": GDALResource(
            exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
            exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
            exe_sozip=os.getenv("EXE_PATH_SOZIP"),
        ),
        "db_connection": database,
        "file_store": FileStoreResource(
            data_dir=str(test_data_dir / "reconstruction_input")
        ),
        "file_store_fastssd": FileStoreResource(
            data_dir=str(test_data_dir / "integration_core")
        ),
        "version": VersionResource("test_version"),
        "validation": ValidationResource(
            exe_val3dity=os.getenv("EXE_PATH_VAL3DITY"),
            exe_cjval=os.getenv("EXE_PATH_CJVAL"),
            exe_cjio=os.getenv("EXE_PATH_CJIO"),
        ),
        "specs": Specs3DBAGResource(),
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
        jobs=[job_nl_reconstruct, job_nl_export, job_nl_export_after_floors],
    )

    with DagsterInstance.ephemeral() as instance:
        resolved_job = defs.get_job_def("nl_reconstruct")
        result = resolved_job.execute_in_process(
            instance=instance, resources=resources, partition_key="10/564/624"
        )

        assert isinstance(result, ExecuteInProcessResult)
        assert result.success

        resolved_job = defs.get_job_def("nl_export")
        result = resolved_job.execute_in_process(
            instance=instance,
            resources=resources,
            run_config={
                "ops": {
                    "reconstruction_output_multitiles_nl": {
                        "config": {"verbose": False, "concurrency": 1}
                    }
                }
            },
        )

        assert isinstance(result, ExecuteInProcessResult)
        assert result.success

        resolved_job = defs.get_job_def("nl_export_after_floors")
        result = resolved_job.execute_in_process(instance=instance, resources=resources)

        assert isinstance(result, ExecuteInProcessResult)
        assert result.success


@pytest.mark.needs_deploy
def test_integration_deploy_release(
    test_data_dir,
    godzilla_server,
    podzilla_server,
    database,
    mock_asset_compressed_tiles,
    mock_asset_compressed_tiles_validation,
    mock_asset_export_index,
    mock_asset_geopackage_nl,
    mock_asset_metadata,
    mock_asset_reconstruction_output_3dtiles_lod12_nl,
    mock_asset_reconstruction_output_3dtiles_lod13_nl,
    mock_asset_reconstruction_output_3dtiles_lod22_nl,
    mock_asset_reconstruction_output_multitiles_nl,
):
    """Can we deploy and release the 3DBAG, everything included?"""

    resources = {
        "version": VersionResource("test_version"),
        "godzilla_server": godzilla_server,
        "podzilla_server": podzilla_server,
        "db_connection": database,
    }

    all_deploy_assets = load_assets_from_package_module(
        deploy, key_prefix="deploy", group_name="deploy"
    )

    all_release_assets = load_assets_from_package_module(
        release, key_prefix="release", group_name="release"
    )

    defs = Definitions(
        resources=resources,
        assets=[
            mock_asset_compressed_tiles,
            mock_asset_compressed_tiles_validation,
            mock_asset_export_index,
            mock_asset_geopackage_nl,
            mock_asset_metadata,
            mock_asset_reconstruction_output_3dtiles_lod12_nl,
            mock_asset_reconstruction_output_3dtiles_lod13_nl,
            mock_asset_reconstruction_output_3dtiles_lod22_nl,
            mock_asset_reconstruction_output_multitiles_nl,
            *all_deploy_assets,
            *all_release_assets,
        ],
        jobs=[job_nl_deploy, job_nl_release],
    )

    with DagsterInstance.ephemeral() as instance:
        resolved_job = defs.get_job_def("nl_deploy")
        result = resolved_job.execute_in_process(instance=instance, resources=resources)

        assert isinstance(result, ExecuteInProcessResult)
        assert result.success

        resolved_job = defs.get_job_def("nl_release")
        result = resolved_job.execute_in_process(
            instance=instance,
            resources=resources,
        )

        assert isinstance(result, ExecuteInProcessResult)
        assert result.success
