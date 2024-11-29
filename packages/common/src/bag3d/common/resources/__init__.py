import os

from bag3d.common.resources.executables import (
    GDALResource,
    PDALResource,
    LASToolsResource,
    TylerResource,
    RooferResource,
    GeoflowResource,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.version import VersionResource

from dagster import EnvVar, get_dagster_logger

logger = get_dagster_logger()

version = VersionResource(os.getenv("BAG3D_RELEASE_VERSION"))


gdal_local = GDALResource(
    exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
    exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
    exe_sozip=os.getenv("EXE_PATH_SOZIP"),
)


pdal_local = PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL"))


db_connection = DatabaseResource(
    host=EnvVar("BAG3D_PG_HOST").get_value(),
    user=EnvVar("BAG3D_PG_USER").get_value(),
    password=EnvVar("BAG3D_PG_PASSWORD").get_value(),
    port=EnvVar("BAG3D_PG_PORT").get_value(),
    dbname=EnvVar("BAG3D_PG_DATABASE").get_value(),
    other_params={"sslmode": EnvVar("BAG3D_PG_SSLMODE").get_value()},
)


file_store = FileStoreResource(
    data_dir=os.getenv("BAG3D_FILESTORE"), dir_id=os.getenv("BAG3D_RELEASE_VERSION")
)
file_store_fastssd = FileStoreResource(
    data_dir=os.getenv("BAG3D_FILESTORE"),
    dir_id=os.getenv("BAG3D_RELEASE_VERSION"),
)


# Configure for gilfoyle
file_store_gilfoyle = FileStoreResource(data_dir="/data")
file_store_gilfoyle_fastssd = FileStoreResource(data_dir="/fastssd/data")


lastools = LASToolsResource(
    exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
    exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
)

tyler = TylerResource(
    exe_tyler=os.getenv("EXE_PATH_TYLER"), exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB")
)

roofer = RooferResource(
    exe_crop=os.getenv("EXE_PATH_ROOFER_CROP"),
    exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"),
)

geoflow = GeoflowResource(
    exe_geoflow=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
    flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"),
)


RESOURCES_LOCAL = {
    "gdal": gdal_local,
    "file_store": file_store,
    "file_store_fastssd": file_store_fastssd,
    "db_connection": db_connection,
    "pdal": pdal_local,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "roofer": roofer,
    "version": version,
}


RESOURCES_PYTEST = {
    "gdal": gdal_local,
    "file_store": file_store,
    "file_store_fastssd": file_store_fastssd,
    "db_connection": db_connection,
    "pdal": pdal_local,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "roofer": roofer,
    "version": version,
}

RESOURCES_PROD = {
    "gdal": gdal_local,
    "file_store": file_store_gilfoyle,
    "file_store_fastssd": file_store_gilfoyle_fastssd,
    "db_connection": db_connection,
    "pdal": pdal_local,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "roofer": roofer,
    "version": version,
}

RESOURCES_DEFAULT = {
    "gdal": GDALResource(),
    "file_store": FileStoreResource(),
    "file_store_fastssd": FileStoreResource(),
    "db_connection": DatabaseResource(),
    "pdal": PDALResource(),
    "lastools": LASToolsResource(),
    "tyler": TylerResource(),
    "geoflow": GeoflowResource(),
    "roofer": RooferResource(),
    "version": VersionResource(),
}


resource_defs_by_env_name = {
    "prod": RESOURCES_PROD,
    "local": RESOURCES_LOCAL,
    "test": RESOURCES_PYTEST,
    "default": RESOURCES_DEFAULT,
    }
env_name = os.getenv("DAGSTER_ENVIRONMENT", "default").lower()
if env_name not in resource_defs_by_env_name.keys():
    logger.warning(f"Invalid environment: {env_name}, setting to default")
    env_name = "default"

resource_defs = resource_defs_by_env_name[env_name]
