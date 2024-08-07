import os
from pathlib import Path

import pytest
from bag3d.common.resources import gdal
from bag3d.common.resources.files import file_store
from bag3d.common.resources.database import DatabaseConnection
from bag3d.common.resources.executables import DOCKER_GDAL_IMAGE
from dagster import build_op_context
from pgutils.connection import PostgresFunctions, PostgresTableIdentifier
from psycopg.sql import SQL, Identifier


LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = "localhost"
PORT = os.getenv("BAG3D_PG_PORT")
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")



@pytest.fixture(scope="function")
def docker_gdal_image():
    """The GDAL docker image to use for the tests"""
    return DOCKER_GDAL_IMAGE


@pytest.fixture(scope="function")
def wkt_testarea():
    """A small test area in the oldtown of Utrecht, incl. the Oudegracht."""
    yield "Polygon ((136251.531 456118.126, 136620.128 456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 456118.126))"


@pytest.fixture
def database():
    db = DatabaseConnection(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME
    )
    yield db


@pytest.fixture
def context(database, docker_gdal_image, wkt_testarea, tmp_path):
    yield build_op_context(
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": ["gebouw", ]
        },
        resources={
            "gdal": gdal.configured({"docker": {"image": docker_gdal_image}}),
            "db_connection": database,
            "file_store": file_store.configured(
                {"data_dir": str(tmp_path), }),
        }
    )

# Setup to add a CLI option to run tests that are marked "slow"
# Ref: https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    else: # pragma: no cover
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


@pytest.fixture(scope="session", autouse=True)
def setenv():
    os.environ["DAGSTER_DEPLOYMENT"] = "pytest"


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)

