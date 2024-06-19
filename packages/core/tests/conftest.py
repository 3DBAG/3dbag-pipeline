import os
from pathlib import Path

import pytest
from bag3d.common.resources.database import DatabaseConnection
from psycopg.sql import SQL, Identifier
from pytest_postgresql import factories

import docker


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
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


LOCAL_DIR = os.getenv('PATH_TO_TEST_DATA')

HOST = "localhost"
PORT = os.getenv('POSTGRES_PORT')
USER = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('POSTGRES_DB')
postgresql_noproc = factories.postgresql_noproc(
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD,
)
# Create the postgresql fixture with a new db - if you want to use existing remove dbname parameter
postgresql = factories.postgresql('postgresql_noproc')


@pytest.fixture
def database(postgresql):
    db = DatabaseConnection(user=USER, password=PASSWORD,
                             host=HOST, port=PORT, dbname=DB_NAME)

    yield db
    query = SQL("""
            DROP SCHEMA IF EXISTS {schema} CASCADE;
        """).format(schema = Identifier("test"))
    db.send_query(query)


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

