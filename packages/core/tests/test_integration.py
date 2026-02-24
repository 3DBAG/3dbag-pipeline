import os

import pytest

from bag3d.common.resources import Specs3DBAGResource, PDALResource
from bag3d.common.resources.executables import (
    GeoflowResource,
    GDALResource,
    TylerResource,
    RooferResource,
    ValidationResource,
    LASToolsResource,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.core.assets import export, reconstruction, ahn, deploy, release
from bag3d.core.jobs import (
    job_nl_export,
    job_nl_export_after_floors,
    job_nl_reconstruct,
    job_ahn_tile_index,
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn_metadata_index,
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
def test_integration_ahn(database, core_file_store):
    """Test the ahn jobs."""
    resources = {
        "lastools": LASToolsResource(
            exe_lasindex=os.getenv("EXE_PATH_LASINDEX", ""),
            exe_las2las=os.getenv("EXE_PATH_LAS2LAS", ""),
            exe_lasinfo=os.getenv("EXE_PATH_LASINFO", ""),
        ),
        "pdal": PDALResource(
            exe_pdal=os.getenv("EXE_PATH_PDAL", ""),
        ),
        "db_connection": database,
        "file_store": FileStoreResource(data_dir=str(core_file_store)),
    }

    all_ahn_assets = load_assets_from_package_module(
        ahn, key_prefix="ahn", group_name="ahn"
    )

    defs = Definitions(
        resources=resources,
        assets=[*all_ahn_assets],
        jobs=[job_ahn_tile_index, job_ahn3, job_ahn4, job_ahn5, job_ahn_metadata_index],
    )

    with DagsterInstance.ephemeral() as instance:
        resolved_job = defs.get_job_def("ahn_tile_index")
        result = resolved_job.execute_in_process(
            instance=instance,
            resources=resources,
        )

        assert isinstance(result, ExecuteInProcessResult)
        assert result.success

        for ahn_version in ("ahn3", "ahn4", "ahn5"):
            resolved_job = defs.get_job_def(ahn_version)
            for partition in ("32bz1", "32bz2"):
                result = resolved_job.execute_in_process(
                    instance=instance,
                    resources=resources,
                    partition_key=partition,
                    run_config={
                        "ops": {
                            f"laz_files_{ahn_version}": {
                                "config": {"force_download": False, "check_hash": False}
                            },
                            f"metadata_{ahn_version}": {
                                "config": {"force": True, "all": True, "verbose": True}
                            },
                            f"lasindex_{ahn_version}": {"config": {"force": True}},
                        }
                    },
                )
                assert isinstance(result, ExecuteInProcessResult)
                assert result.success

        resolved_job = defs.get_job_def("ahn_metadata_index")
        result = resolved_job.execute_in_process(
            instance=instance,
            resources=resources,
        )

        assert isinstance(result, ExecuteInProcessResult)
        assert result.success


@pytest.mark.needs_tools
def test_integration_reconstruction_and_export(
    database,
    test_data_dir,
    core_file_store,
    core_file_store_fastssd,
    mock_asset_reconstruction_input,
    mock_asset_tiles,
    mock_asset_index,
    mock_asset_metadata_ahn3_index,
    mock_asset_metadata_ahn4_index,
    mock_asset_metadata_ahn5_index,
    configured_mock_asset_io_manager,
):
    # update quadtree
    og_quadtree = test_data_dir / "quadtree.tsv"
    export_dir = core_file_store / "3DBAG" / "export_test_version"
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
        "file_store": FileStoreResource(data_dir=str(core_file_store)),
        "file_store_fastssd": FileStoreResource(data_dir=str(core_file_store_fastssd)),
        "version": ReleaseVersionResource(version="test_version"),
        "validation": ValidationResource(
            exe_val3dity=os.getenv("EXE_PATH_VAL3DITY"),
            exe_cjval=os.getenv("EXE_PATH_CJVAL"),
            exe_cjio=os.getenv("EXE_PATH_CJIO"),
        ),
        "specs": Specs3DBAGResource(),
        "mock_asset_io_manager": configured_mock_asset_io_manager,
    }

    all_reconstruction_assets = load_assets_from_package_module(
        reconstruction, key_prefix="reconstruction", group_name="reconstruction"
    )

    # Filter the assets to include only the ones we need
    reconstruction_assets = [
        asset
        for asset in all_reconstruction_assets
        if asset.key  # type: ignore[union-attr]
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
            mock_asset_reconstruction_input,
            mock_asset_tiles,
            mock_asset_index,
            mock_asset_metadata_ahn3_index,
            mock_asset_metadata_ahn4_index,
            mock_asset_metadata_ahn5_index,
            *reconstruction_assets,
            *all_export_assets,
        ],
        jobs=[job_nl_reconstruct, job_nl_export, job_nl_export_after_floors],
    )

    with DagsterInstance.ephemeral() as instance:
        resolved_job = defs.get_job_def("nl_reconstruct")
        result = resolved_job.execute_in_process(
            instance=instance,
            resources=resources,
            partition_key="10/564/624",
            run_config={
                "ops": {
                    "reconstructed_building_models_nl": {"config": {"loglevel": "info"}}
                }
            },
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
    configured_mock_asset_io_manager,
):
    """Can we deploy and release the 3DBAG, everything included?"""

    resources = {
        "version": ReleaseVersionResource(version="test_version"),
        "godzilla_server": godzilla_server,
        "podzilla_server": podzilla_server,
        "db_connection": database,
        "mock_asset_io_manager": configured_mock_asset_io_manager,
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
