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
from bag3d.common.resources.files import file_store
from bag3d.common.resources.database import db_connection


# The 'mount_point' is the directory in the container that is bind-mounted on the host

gdal_local = GDALResource(
    docker_cfg=DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp")
).app


gdal_prod = GDALResource(
    exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
    exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
    exe_sozip=os.getenv("EXE_PATH_SOZIP"),
).app


pdal_local = PDALResource(
    docker_cfg=DockerConfig(image=DOCKER_PDAL_IMAGE, mount_point="/tmp")
).app

pdal_prod = PDALResource = PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL")).app


db_connection_docker = db_connection.configured(
    {
        "port": int(os.getenv("BAG3D_PG_PORT", 5432)),
        "user": os.getenv("BAG3D_PG_USER"),
        "password": os.getenv("BAG3D_PG_PASSWORD"),
        "dbname": os.getenv("BAG3D_PG_DATABASE"),
        "host": os.getenv("BAG3D_PG_HOST"),
        # , "sslmode": os.getenv("BAG3D_PG_SSLMODE", "allow"),
    }
)


# Configure for gilfoyle
file_store_gilfoyle = file_store.configured({"data_dir": "/data"})
file_store_gilfoyle_fastssd = file_store.configured({"data_dir": "/fastssd/data"})


lastools = LASToolsResource(
    exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
    exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
).app

tyler = TylerResource(
    exe_tyler=os.getenv("EXE_PATH_TYLER"), exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB")
).app

roofer = RooferResource(exe_roofer_crop=os.getenv("EXE_PATH_ROOFER_CROP")).app

geoflow = GeoflowResource(
    exe_geoflow=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
    flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"),
).app

RESOURCES_LOCAL = {
    "gdal": gdal_local,
    "file_store": file_store,
    "file_store_fastssd": file_store,
    "db_connection": db_connection_docker,
    "pdal": pdal_local,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "roofer": roofer,
}


RESOURCES_PYTEST = {
    "gdal": gdal_local,
    "file_store": file_store,
    "file_store_fastssd": file_store,
    "db_connection": db_connection_docker,
    "pdal": pdal_local,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "roofer": roofer,
}

RESOURCES_PROD = {
    "gdal": gdal_prod,
    "file_store": file_store_gilfoyle,
    "file_store_fastssd": file_store_gilfoyle_fastssd,
    "db_connection": db_connection_docker,
    "pdal": pdal_prod,
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
