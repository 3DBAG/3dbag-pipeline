import os

from bag3d.common.resources.executables import (
    GDALResource,
    PDALResource,
    DockerConfig,
    LASToolsResource,
    TylerResource,
    RooferResource,
    GeoflowResource,
    DOCKER_GDAL_IMAGE,
    DOCKER_PDAL_IMAGE,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.database import DatabaseResource

from dagster import EnvVar

# The 'mount_point' is the directory in the container that is bind-mounted on the host

gdal_docker = GDALResource(
    docker_cfg=DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp")
)


gdal_local = GDALResource(
    exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
    exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
    exe_sozip=os.getenv("EXE_PATH_SOZIP"),
)


pdal_docker = PDALResource(
    docker_cfg=DockerConfig(image=DOCKER_PDAL_IMAGE, mount_point="/tmp")
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
    data_dir="/tmp", dir_id=os.getenv("BAG3D_RELEASE_VERSION")
)
file_store_fastssd = FileStoreResource(
    data_dir="/tmp",
    dir_id=os.getenv("BAG3D_RELEASE_VERSION"),
)


# Configure for gilfoyle
file_store_gilfoyle = FileStoreResource(
    data_dir="/data", dir_id=os.getenv("BAG3D_RELEASE_VERSION")
)
file_store_gilfoyle_fastssd = FileStoreResource(
    data_dir="/fastssd/data",
    dir_id=os.getenv("BAG3D_RELEASE_VERSION"),
)


lastools = LASToolsResource(
    exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
    exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
)

tyler = TylerResource(
    exe_tyler=os.getenv("EXE_PATH_TYLER"), exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB")
)

roofer = RooferResource(exe_roofer_crop=os.getenv("EXE_PATH_ROOFER_CROP"))

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
}

# Resource definitions for import

resource_defs_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "local": RESOURCES_LOCAL,
    "pytest": RESOURCES_PYTEST,
}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
resource_defs = resource_defs_by_deployment_name[deployment_name]
