import os
from pathlib import Path

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
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.version import VersionResource
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.server_transfer import ServerTransferResource

from dagster import EnvVar, get_dagster_logger

logger = get_dagster_logger()

version = VersionResource(os.getenv("BAG3D_RELEASE_VERSION"))

specs = Specs3DBAGResource()

gdal = GDALResource(
    exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
    exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
    exe_sozip=os.getenv("EXE_PATH_SOZIP"),
)


pdal = PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL"))


db_connection = DatabaseResource(
    host=EnvVar("BAG3D_PG_HOST").get_value(),
    user=EnvVar("BAG3D_PG_USER").get_value(),
    password=EnvVar("BAG3D_PG_PASSWORD").get_value(),
    port=EnvVar("BAG3D_PG_PORT").get_value(),
    dbname=EnvVar("BAG3D_PG_DATABASE").get_value(),
    other_params={"sslmode": EnvVar("BAG3D_PG_SSLMODE").get_value()},
)


godzilla_server = ServerTransferResource(
    host=EnvVar("BAG3D_GODZILLA_HOST").get_value(),
    user=EnvVar("BAG3D_GODZILLA_USER").get_value(),
    target_dir=EnvVar("BAG3D_GODZILLA_TARGET_DIR").get_value(),
    public_dir=EnvVar("BAG3D_GODZILLA_PUBLIC_DIR").get_value(),
)

podzilla_server = ServerTransferResource(
    host=EnvVar("BAG3D_PODZILLA_HOST").get_value(),
    user=EnvVar("BAG3D_PODZILLA_USER").get_value(),
    target_dir=EnvVar("BAG3D_PODZILLA_TARGET_DIR").get_value(),
)

file_store = FileStoreResource(data_dir=os.getenv("BAG3D_FILESTORE"))
file_store_fastssd = FileStoreResource(data_dir=os.getenv("BAG3D_FILESTORE_FASTSSD"))


# Configure for  gilfoyle
file_store_gilfoyle = FileStoreResource(data_dir="/data")
file_store_gilfoyle_fastssd = FileStoreResource(data_dir="/fastssd/data")

lastools = LASToolsResource(
    exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
    exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
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


resource_defs = {
    "gdal": gdal,
    "file_store": file_store,
    "file_store_fastssd": file_store_fastssd,
    "db_connection": db_connection,
    "pdal": pdal,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "validation": validation,
    "roofer": roofer,
    "version": version,
    "specs": specs,
    "godzilla_server": godzilla_server,
    "podzilla_server": podzilla_server,
}


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
#
# RESOURCES_DEFAULT = {
#     "gdal": GDALResource(),
#     "file_store": FileStoreResource(),
#     "file_store_fastssd": FileStoreResource(),
#     "db_connection": DatabaseResource(),
#     "pdal": PDALResource(),
#     "lastools": LASToolsResource(),
#     "tyler": TylerResource(),
#     "geoflow": GeoflowResource(),
#     "validation": ValidationResource(),
#     "roofer": RooferResource(),
#     "version": VersionResource(),
#     "specs": specs,
#     "godzilla_server": ServerTransferResource(),
#     "podzilla_server": ServerTransferResource(),
# }
#
#
# resource_defs_by_env_name = {
#     "prod": RESOURCES_PROD,
#     "local": RESOURCES_LOCAL,
#     "test": RESOURCES_TEST,
#     "default": RESOURCES_DEFAULT,
# }
#
# env_name = os.getenv("DAGSTER_ENVIRONMENT", "default").lower()
# if env_name not in resource_defs_by_env_name:
#     logger.warning(f"Invalid environment: {env_name}, setting to default")
#     env_name = "default"
#
# resource_defs = resource_defs_by_env_name[env_name]
