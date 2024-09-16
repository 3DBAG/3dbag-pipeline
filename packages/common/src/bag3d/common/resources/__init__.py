import os

from bag3d.common.resources.executables import (
    GDALResources,
    PDALResources,
    DockerConfig,
    LASToolsResource,
    tyler,
    geoflow,
    roofer,
    DOCKER_GDAL_IMAGE,
    DOCKER_PDAL_IMAGE,
)
from bag3d.common.resources.files import file_store
from bag3d.common.resources.database import db_connection

# Local config ---

# The 'mount_point' is the directory in the container that is bind-mounted on the host

gdal_local = GDALResources(
    docker_cfg=DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp")
)


gdal_prod = GDALResources(
    exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
    exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
    exe_sozip=os.getenv("EXE_PATH_SOZIP"),
)


pdal_local = PDALResources(
    docker_cfg=DockerConfig(image=DOCKER_PDAL_IMAGE, mount_point="/tmp")
)

pdal_prod = PDALResources = PDALResources(exe_pdal=os.getenv("EXE_PATH_PDAL"))


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


# Production config ---

# Configure for gilfoyle
file_store_gilfoyle = file_store.configured({"data_dir": "/data"})
file_store_gilfoyle_fastssd = file_store.configured({"data_dir": "/fastssd/data"})


lastools = LASToolsResource(
    exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
    exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
)

tyler_prod = tyler.configured(
    {
        "exes": {
            "tyler-db": os.getenv("EXE_PATH_TYLER_DB"),
            "tyler": os.getenv("EXE_PATH_TYLER"),
        }
    }
)

roofer_prod = roofer.configured(
    {
        "exes": {"crop": os.getenv("EXE_PATH_ROOFER_CROP")},
    }
)

geoflow_prod = geoflow.configured(
    {
        "exes": {"geof": os.getenv("EXE_PATH_ROOFER_RECONSTRUCT")},
        "flowcharts": {"reconstruct": os.getenv("FLOWCHART_PATH_RECONSTRUCT")},
    }
)

RESOURCES_LOCAL = {
    "gdal": gdal_local,
    "file_store": file_store,
    "file_store_fastssd": file_store,
    "db_connection": db_connection_docker,
    "pdal": pdal_local,
    "lastools": lastools,
    "tyler": tyler_prod,
    "geoflow": geoflow_prod,
    "roofer": roofer_prod,
}

# pytest config ---

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
    "tyler": tyler_prod,
    "geoflow": geoflow_prod,
    "roofer": roofer_prod,
}

# Resource definitions for import

resource_defs_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "local": RESOURCES_LOCAL,
    "pytest": RESOURCES_PYTEST,
}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
resource_defs = resource_defs_by_deployment_name[deployment_name]
