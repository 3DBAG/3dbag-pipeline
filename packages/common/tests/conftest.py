import os
from pathlib import Path

import pytest
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
def context(database):
    yield build_op_context(resources={"db_connection": database})


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
