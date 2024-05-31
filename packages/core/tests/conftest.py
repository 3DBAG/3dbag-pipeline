from time import sleep
from pathlib import Path
import string
from random import choice
from shutil import rmtree
import os

import pytest
import docker

from pathlib import Path

from pytest_postgresql import factories


from bag3d.common.resources.database import DatabaseConnection

RES_CONTAINER_ID = "pytest-3dbag-pipeline-db_connection"

LOCAL_DIR = "/Users/localadmin/Repos/3dbag-pipeline/docker/data/test_data"

HOST = "localhost"
PORT = 5432
USER = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('POSTGRES_DB')
postgresql_noproc = factories.postgresql_noproc(
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD,
)
# Create the postgresql fixture with a new db - if you want ot use existing remove dbname parameter
#postgresql = factories.postgresql('postgresql_noproc', dbname=DB_NAME)
postgresql = factories.postgresql('postgresql_noproc')


@pytest.fixture
def database(postgresql):
    db = DatabaseConnection(user=USER, password=PASSWORD,
                             host=HOST, port=PORT, dbname=DB_NAME)

    yield db


@pytest.fixture(scope="session", autouse=True)
def setenv():
    os.environ["DAGSTER_DEPLOYMENT"] = "pytest"


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)






@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="function")
def wkt_testarea():
    """A small test area in the oldtown of Utrecht, incl. the Oudegracht."""
    yield "Polygon ((136251.531 456118.126, 136620.128 456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 456118.126))"


@pytest.fixture(scope="session")
def docker_gdal_image():
    """The GDAL docker image to use for the tests"""
    return "ghcr.io/osgeo/gdal:alpine-small-latest"


@pytest.fixture(scope="function")
def temp_file_store():
    r = ''.join(choice(string.ascii_letters) for _ in range(8))
    tmp = Path(f"/tmp/tmp_3dbag_test_{r}")
    tmp.mkdir()
    tmp.chmod(mode=0o777)
    yield tmp
    rmtree(str(tmp))
