import os
from pathlib import Path

import pytest
from bag3d.common.resources import gdal
from bag3d.common.resources.files import file_store
from bag3d.common.resources.database import DatabaseConnection
from dagster import build_op_context
from pgutils.connection import PostgresFunctions, PostgresTableIdentifier
from psycopg.sql import SQL, Identifier
from pytest_postgresql import factories

import docker

LOCAL_DIR = os.getenv("PATH_TO_TEST_DATA")
HOST = "localhost"
PORT = os.getenv("POSTGRES_PORT")
USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
postgresql_noproc = factories.postgresql_noproc(
    host=HOST, port=PORT, user=USER, password=PASSWORD
)
postgresql = factories.postgresql("postgresql_noproc", dbname="test")


@pytest.fixture(scope="function")
def docker_gdal_image():
    """The GDAL docker image to use for the tests"""
    return "ghcr.io/osgeo/gdal:alpine-small-latest"


@pytest.fixture(scope="function")
def wkt_testarea():
    """A small test area in the oldtown of Utrecht, incl. the Oudegracht."""
    yield "Polygon ((136251.531 456118.126, 136620.128 456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 456118.126))"


@pytest.fixture
def database(postgresql):
    db = DatabaseConnection(conn=postgresql)
    PostgresFunctions(db)
    tbl = PostgresTableIdentifier("public", "existing_table")
    query = SQL(
        """CREATE TABLE {table} (id INTEGER, value TEXT);
                   INSERT INTO {table} VALUES (1, 'bla');
                   INSERT INTO {table} VALUES (2, 'foo');"""
    ).format(table=Identifier(tbl.schema.str, tbl.table.str))

    db.send_query(query)
    yield db
    db.conn.rollback()


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


@pytest.fixture
def baseregisters_database():
    db = DatabaseConnection(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME
    )
    yield db
    # db.conn.rollback()


@pytest.fixture
def baseregisters_context(baseregisters_database, docker_gdal_image, wkt_testarea, tmp_path):
    yield build_op_context(
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": ["gebouw", ]
        },
        resources={
            "gdal": gdal.configured({"docker": {"image": docker_gdal_image}}),
            "db_connection": baseregisters_database,
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

