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
from bag3d.common.resources.version import VersionResource
from bag3d.core.assets import export, reconstruction, ahn
from bag3d.core.jobs import (
    job_nl_export,
    job_nl_export_after_floors,
    job_nl_reconstruct,
    job_ahn_tile_index,
    job_ahn3,
    job_ahn4,
    job_ahn5, job_ahn_metadata_index,
)
from dagster import (
    AssetKey,
    Definitions,
    ExecuteInProcessResult,
    load_assets_from_package_module,
    DagsterInstance,
)


@pytest.mark.needs_tools
def test_integration_ahn(database, test_data_dir):
    """Test the ahn jobs."""
    resources = {
        "lastools": LASToolsResource(
            exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
            exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
        ),
        "pdal": PDALResource(
            exe_pdal=os.getenv("EXE_PATH_PDAL"),
        ),
        "db_connection": database,
        "file_store": FileStoreResource(
            data_dir=str(test_data_dir / "reconstruction_input")
        ),
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
                                "config": {"force": True, "all": True}
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
    mock_asset_reconstruction_input,
    mock_asset_tiles,
    mock_asset_index,
    mock_asset_metadata_ahn3,
    mock_asset_metadata_ahn4,
    mock_asset_metadata_ahn5,
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
            mock_asset_reconstruction_input,
            mock_asset_tiles,
            mock_asset_index,
            mock_asset_metadata_ahn3,
            mock_asset_metadata_ahn4,
            mock_asset_metadata_ahn5,
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
                    "reconstructed_building_models_nl": {
                        "config": {"loglevel": "debug"}
                    }
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
