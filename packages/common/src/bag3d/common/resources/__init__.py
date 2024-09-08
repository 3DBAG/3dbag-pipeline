import os

from bag3d.common.resources.executables import gdal, pdal, lastools, tyler, geoflow, roofer, DOCKER_GDAL_IMAGE
from bag3d.common.resources.files import file_store
from bag3d.common.resources.database import db_connection

from bag3d.common.resources.temp_until_configurableresource import (
EXE_PATH_TYLER, EXE_PATH_TYLER_DB, EXE_PATH_ROOFER_CROP, EXE_PATH_ROOFER_RECONSTRUCT,
FLOWCHART_PATH_RECONSTRUCT, EXE_PATH_OGR2OGR, EXE_PATH_OGRINFO, EXE_PATH_PDAL, EXE_PATH_LASINDEX,
    EXE_PATH_LAS2LAS
)

# Local config ---

# The 'mount_point' is the directory in the container that is bind-mounted on the host
gdal_local = gdal.configured({
    "docker": {
        "image": DOCKER_GDAL_IMAGE,
        "mount_point": "/tmp"
    }
})


db_connection_docker = db_connection.configured({
    "port": int(os.getenv("BAG3D_PG_PORT", 5432)),
    "user": os.getenv("BAG3D_PG_USER"),
    "password": os.getenv("BAG3D_PG_PASSWORD"),
    "dbname": os.getenv("BAG3D_PG_DATABASE"),
    "host": os.getenv("BAG3D_PG_HOST")
    # , "sslmode": os.getenv("BAG3D_PG_SSLMODE", "allow"),
})


# Production config ---

# Configure for gilfoyle
file_store_gilfoyle = file_store.configured({"data_dir": "/data"})
file_store_gilfoyle_fastssd = file_store.configured({"data_dir": "/fastssd/data"})

gdal_prod = gdal.configured({
    "exes": {
        "ogr2ogr": EXE_PATH_OGR2OGR,
        "ogrinfo": EXE_PATH_OGRINFO,
        "sozip": EXE_PATH_OGRINFO,
    }
})

pdal_prod = pdal.configured({
    "exes": {
        "pdal": EXE_PATH_PDAL
    }
})

lastools_prod = lastools.configured({
    "exes": {
        "lasindex": EXE_PATH_LASINDEX,
        "las2las": EXE_PATH_LAS2LAS
    }
})

tyler_prod = tyler.configured({
    "exes": {
        "tyler-db": EXE_PATH_TYLER_DB,
        "tyler": EXE_PATH_TYLER
    }
})

roofer_prod = roofer.configured({
    "exes": {
        "crop": EXE_PATH_ROOFER_CROP
    },
})

geoflow_prod = geoflow.configured({
    "exes": {
        "geof": EXE_PATH_ROOFER_RECONSTRUCT
    },
    "flowcharts": {
        "reconstruct": FLOWCHART_PATH_RECONSTRUCT
    }
})

RESOURCES_LOCAL = {
    "gdal": gdal,
    "file_store": file_store,
    "file_store_fastssd": file_store,
    "db_connection": db_connection_docker,
    "pdal": pdal,
    "lastools": lastools,
    "tyler": tyler_prod,
    "geoflow": geoflow_prod,
    "roofer": roofer_prod
}

# pytest config ---

RESOURCES_PYTEST = {
    "gdal": gdal_local,
    "file_store": file_store,
    "file_store_fastssd": file_store,
    "db_connection": db_connection_docker,
    "pdal": pdal,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "roofer": roofer
}

RESOURCES_PROD = {
    "gdal": gdal_prod,
    "file_store": file_store_gilfoyle,
    "file_store_fastssd": file_store_gilfoyle_fastssd,
    "db_connection": db_connection_docker,
    "pdal": pdal_prod,
    "lastools": lastools_prod,
    "tyler": tyler_prod,
    "geoflow": geoflow_prod,
    "roofer": roofer_prod
}

# Resource definitions for import

resource_defs_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "local": RESOURCES_LOCAL,
    "pytest": RESOURCES_PYTEST
}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
resource_defs = resource_defs_by_deployment_name[deployment_name]
