import os
from pathlib import Path

import pytest

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import (
    GDALResource,
    PDALResource,
)
from dagster import build_asset_context

pytest_plugins = ["bag3d.common.testing.conftest_plugin"]

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA", "")
HOST = os.getenv("BAG3D_PG_HOST", "")
PORT = int(os.getenv("BAG3D_PG_PORT", "5432"))
USER = os.getenv("BAG3D_PG_USER", "")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD", "")
DB_NAME = os.getenv("BAG3D_PG_DATABASE", "")


@pytest.fixture(scope="session")
def gdal():
    yield GDALResource(
        exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
        exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
        exe_sozip=os.getenv("EXE_PATH_SOZIP"),
    )


@pytest.fixture(scope="session")
def pdal():
    yield PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL"))


@pytest.fixture(scope="function")
def wkt_testarea():
    """A small test area in the oldtown of Utrecht, incl. the Oudegracht."""
    yield "Polygon ((136251.531 456118.126, 136620.128 456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 456118.126))"


@pytest.fixture
def context(wkt_testarea):
    yield build_asset_context()


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)


@pytest.fixture
def database():
    """Live database connection for tests that require a real database."""
    db = DatabaseResource(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME
    )
    yield db


@pytest.fixture
def resources(database, file_store, gdal):
    return {
        "gdal": gdal,
        "computation_db": database,
        "file_store": file_store,
        "version": "test_version",
    }
