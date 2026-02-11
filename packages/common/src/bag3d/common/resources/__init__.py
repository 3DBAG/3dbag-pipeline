import os

from dagster import EnvVar, get_dagster_logger

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import (
    GDALResource,
    PDALResource,
    LASToolsResource,
    TylerResource,
    RooferResource,
    GeoflowResource,
    ValidationResource,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.server_transfer import ServerTransferResource
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.tool_versions import ToolVersionsResource
from bag3d.common.resources.version import VersionResource

# NOTE os.getenv() shows the env value in the Dagster UI, EnvVar hides the value in the Dagster UI

logger = get_dagster_logger()

version = VersionResource(os.getenv("BAG3D_RELEASE_VERSION"))

specs = Specs3DBAGResource()

# Tool versions resource - instantiated at import time for code_version access
tool_versions = ToolVersionsResource(
    exe_tyler=os.getenv("EXE_PATH_TYLER"),
    exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"),
    exe_tyler_multiformat=os.getenv("EXE_PATH_TYLER_MULTIFORMAT"),
    exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"),
    exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
    exe_pdal=os.getenv("EXE_PATH_PDAL"),
    exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
    exe_geof=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
)


def resources_by_deployment(dagster_deployment: str) -> dict:
    configure_at_run_launch = False
    configure_from_env = False
    if dagster_deployment.lower() == "default":
        configure_at_run_launch = True
    elif dagster_deployment.lower() in ["production", "test_docker"]:
        configure_from_env = True
    else:
        logger.warning(
            f"Invalid DAGSTER_DEPLOYMENT {dagster_deployment}, defaulting to 'default'"
        )
        configure_at_run_launch = True

    if configure_at_run_launch:
        return {
            "gdal": GDALResource.configure_at_launch(),
            "file_store": FileStoreResource.configure_at_launch(),
            "file_store_fastssd": FileStoreResource.configure_at_launch(),
            "db_connection": DatabaseResource.configure_at_launch(),
            "pdal": PDALResource.configure_at_launch(),
            "lastools": LASToolsResource.configure_at_launch(),
            "tyler": TylerResource.configure_at_launch(),
            "geoflow": GeoflowResource.configure_at_launch(),
            "validation": ValidationResource.configure_at_launch(),
            "roofer": RooferResource.configure_at_launch(),
            "version": version,
            "specs": specs,
            "godzilla_server": ServerTransferResource.configure_at_launch(),
            "podzilla_server": ServerTransferResource.configure_at_launch(),
        }
    elif configure_from_env:
        return {
            "gdal": GDALResource(
                exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
                exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
                exe_sozip=os.getenv("EXE_PATH_SOZIP"),
            ),
            "file_store": FileStoreResource(data_dir=os.getenv("BAG3D_FILESTORE")),
            "file_store_fastssd": FileStoreResource(
                data_dir=os.getenv("BAG3D_FILESTORE_FASTSSD")
            ),
            "db_connection": DatabaseResource(
                host=EnvVar("BAG3D_PG_HOST"),
                user=EnvVar("BAG3D_PG_USER"),
                password=EnvVar("BAG3D_PG_PASSWORD"),
                port=EnvVar.int("BAG3D_PG_PORT"),
                dbname=EnvVar("BAG3D_PG_DATABASE"),
                other_params={"sslmode": EnvVar("BAG3D_PG_SSLMODE")},
            ),
            "pdal": PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL")),
            "lastools": LASToolsResource(
                exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
                exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
                exe_lasinfo=os.getenv("EXE_PATH_LASINFO"),
            ),
            "tyler": TylerResource(
                exe_tyler=os.getenv("EXE_PATH_TYLER"),
                exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"),
                exe_tyler_multiformat=os.getenv("EXE_PATH_TYLER_MULTIFORMAT"),
            ),
            "geoflow": GeoflowResource(
                exe_geoflow=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
                flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"),
            ),
            "validation": ValidationResource(
                exe_val3dity=os.getenv("EXE_PATH_VAL3DITY"),
                exe_cjval=os.getenv("EXE_PATH_CJVAL"),
                exe_cjio=os.getenv("EXE_PATH_CJIO"),
            ),
            "roofer": RooferResource(
                exe_crop=os.getenv("EXE_PATH_ROOFER_CROP"),
                exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"),
            ),
            "version": version,
            "specs": specs,
            "godzilla_server": ServerTransferResource(
                host=EnvVar("BAG3D_GODZILLA_HOST"),
                user=EnvVar("BAG3D_GODZILLA_USER"),
                target_dir=EnvVar("BAG3D_GODZILLA_TARGET_DIR"),
                public_dir=EnvVar("BAG3D_GODZILLA_PUBLIC_DIR"),
            ),
            "podzilla_server": ServerTransferResource(
                host=EnvVar("BAG3D_PODZILLA_HOST"),
                user=EnvVar("BAG3D_PODZILLA_USER"),
                target_dir=EnvVar("BAG3D_PODZILLA_TARGET_DIR"),
            ),
        }
    else:
        raise RuntimeError("Cannot configure dagster environment")


env_name = os.getenv("DAGSTER_DEPLOYMENT", "default")
resource_defs = resources_by_deployment(env_name)
