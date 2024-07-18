import os

from bag3d.common.resources.executables import gdal, partialzip, pdal, lastools, tyler, geoflow, roofer
from bag3d.common.resources.files import FileStore, file_store
from bag3d.common.resources.database import db_connection, container
# from bag3d.common.simple_for_testing import conf_simpl_dock

from bag3d.common.resources.temp_until_configurableresource import (
EXE_PATH_TYLER, EXE_PATH_TYLER_DB, EXE_PATH_ROOFER_CROP, EXE_PATH_GEOF,
FLOWCHART_PATH_RECONSTRUCT
)

# Local config ---

# The 'mount_point' is the directory in the container that is bind-mounted on the host
gdal_local = gdal.configured({
    "docker": {
        "image": "osgeo/gdal:alpine-small-3.5.2",
        "mount_point": "/tmp"
    }
})

pguser = "db3dbag_user"
pgpassword = str("db3dbag_1234")
pgdatabase = "baseregisters"

db_connection_docker = db_connection.configured({
    "docker": {
        "image_id": "balazsdukai/3dbag-sample-data:base",
    },
    "port": 5561,
    "user": pguser,
    "password": pgpassword,
    "dbname": pgdatabase
})


# The local resources are set up to run with the 3dbag-sample-data docker image
RESOURCES_LOCAL = {
    "gdal": gdal,
    "file_store": file_store,
    "file_store_fastssd": file_store,
    "db_connection": db_connection_docker,
    "container": container,
    "pdal": pdal,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "roofer": roofer
}

# pytest config ---

RESOURCES_PYTEST = {
    "gdal": gdal_local,
    "file_store": file_store,
    "file_store_fastssd": file_store,
    "db_connection": db_connection_docker,
    "container": container,
    "pdal": pdal,
    "lastools": lastools,
    "tyler": tyler,
    "geoflow": geoflow,
    "roofer": roofer
}

# Production config ---

# Configure for gilfoyle
file_store_gilfoyle = file_store.configured({"data_dir": "/data"})
file_store_gilfoyle_fastssd = file_store.configured({"data_dir": "/fastssd/data"})

gdal_prod = gdal.configured({
    "exes": {
        "ogr2ogr": "/opt/bin/ogr2ogr",
        "ogrinfo": "/opt/bin/ogrinfo",
        "sozip": "/opt/bin/sozip"
    }
})

pdal_prod = pdal.configured({
    "exes": {
        "pdal": "/opt/bin/pdal"
    }
})

lastools_prod = lastools.configured({
    "exes": {
        "lasindex": "/opt/bin/lasindex64",
        "las2las": "/opt/bin/las2las64"
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
        "geof": EXE_PATH_GEOF
    },
    "flowcharts": {
        "reconstruct": FLOWCHART_PATH_RECONSTRUCT
    }
})

db_connection_from_env = db_connection.configured({
    "port": int(os.environ.get("DAGSTER_DB_CONNECTION_PORT", 5432)),
    "user": os.environ.get("DAGSTER_DB_CONNECTION_USER"),
    "password": os.environ.get("DAGSTER_DB_CONNECTION_PASSWORD"),
    "dbname": os.environ.get("DAGSTER_DB_CONNECTION_DBNAME"),
    "host": os.environ.get("DAGSTER_DB_CONNECTION_HOST"),
})

RESOURCES_PROD = {
    "gdal": gdal_prod,
    "file_store": file_store_gilfoyle,
    "file_store_fastssd": file_store_gilfoyle_fastssd,
    "db_connection": db_connection_from_env,
    "container": container,
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
