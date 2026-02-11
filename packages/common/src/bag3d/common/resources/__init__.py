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
from bag3d.common.resources.version import VersionResource
from bag3d.common.resources.tool_versions import ToolVersionsResource

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

gdal = GDALResource(
    exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
    exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
    exe_sozip=os.getenv("EXE_PATH_SOZIP"),
)

pdal = PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL"))

# db_connection = DatabaseResource(
#     host=EnvVar("BAG3D_PG_HOST"),
#     user=EnvVar("BAG3D_PG_USER"),
#     password=EnvVar("BAG3D_PG_PASSWORD"),
#     port=EnvVar("BAG3D_PG_PORT"),
#     dbname=EnvVar("BAG3D_PG_DATABASE"),
#     other_params={"sslmode": EnvVar("BAG3D_PG_SSLMODE")},
# )


godzilla_server = ServerTransferResource(
    host=EnvVar("BAG3D_GODZILLA_HOST"),
    user=EnvVar("BAG3D_GODZILLA_USER"),
    target_dir=EnvVar("BAG3D_GODZILLA_TARGET_DIR"),
    public_dir=EnvVar("BAG3D_GODZILLA_PUBLIC_DIR"),
)

podzilla_server = ServerTransferResource(
    host=EnvVar("BAG3D_PODZILLA_HOST"),
    user=EnvVar("BAG3D_PODZILLA_USER"),
    target_dir=EnvVar("BAG3D_PODZILLA_TARGET_DIR"),
)

file_store = FileStoreResource(data_dir=os.getenv("BAG3D_FILESTORE"))
file_store_fastssd = FileStoreResource(data_dir=os.getenv("BAG3D_FILESTORE_FASTSSD"))

lastools = LASToolsResource(
    exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
    exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
    exe_lasinfo=os.getenv("EXE_PATH_LASINFO"),
)

tyler = TylerResource(
    exe_tyler=os.getenv("EXE_PATH_TYLER"),
    exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"),
    exe_tyler_multiformat=os.getenv("EXE_PATH_TYLER_MULTIFORMAT"),
)

roofer = RooferResource(
    exe_crop=os.getenv("EXE_PATH_ROOFER_CROP"),
    exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"),
)

geoflow = GeoflowResource(
    exe_geoflow=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
    flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"),
)

validation = ValidationResource(
    exe_val3dity=os.getenv("EXE_PATH_VAL3DITY"),
    exe_cjval=os.getenv("EXE_PATH_CJVAL"),
    exe_cjio=os.getenv("EXE_PATH_CJIO"),
)


# resource_defs = {
#     "gdal": gdal,
#     "file_store": file_store,
#     "file_store_fastssd": file_store_fastssd,
#     "db_connection": db_connection,
#     "pdal": pdal,
#     "lastools": lastools,
#     "tyler": tyler,
#     "geoflow": geoflow,
#     "validation": validation,
#     "roofer": roofer,
#     "version": version,
#     "specs": specs,
#     "godzilla_server": godzilla_server,
#     "podzilla_server": podzilla_server,
# }


# RESOURCES_PROD = {
#     "gdal": gdal,
#     "file_store": file_store_gilfoyle,
#     "file_store_fastssd": file_store_gilfoyle_fastssd,
#     "db_connection": db_connection,
#     "pdal": pdal,
#     "lastools": lastools,
#     "tyler": tyler,
#     "geoflow": geoflow,
#     "validation": validation,
#     "roofer": roofer,
#     "version": version,
#     "specs": specs,
#     "godzilla_server": godzilla_server,
#     "podzilla_server": podzilla_server,
# }


def resources_by_deployment() -> dict:
    return {
        "default": {
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
            "version": VersionResource(),
            "specs": specs,
            "godzilla_server": ServerTransferResource.configure_at_launch(),
            "podzilla_server": ServerTransferResource.configure_at_launch(),
        },
        "production": {},
        "test_docker": {},
    }


env_name = os.getenv("DAGSTER_DEPLOYMENT", "default").lower()
resource_defs = resources_by_deployment()[env_name]
