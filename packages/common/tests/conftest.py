import os
from pathlib import Path

import pytest

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import (
    DOCKER_GDAL_IMAGE,
    DOCKER_PDAL_IMAGE,
    GDALResource,
    PDALResource,
    DockerConfig,
)
from bag3d.common.resources.files import FileStoreResource
from dagster import build_op_context

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = "localhost"
PORT = os.getenv("BAG3D_PG_PORT")
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")


@pytest.fixture(scope="session")
def docker_config():
    yield DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp")


@pytest.fixture(scope="session")
def gdal(docker_config):
    yield GDALResource(docker_cfg=docker_config)


@pytest.fixture(scope="session")
def pdal(docker_config):
    yield PDALResource(docker_cfg=docker_config)


@pytest.fixture(scope="function")
def wkt_testarea():
    """A small test area in the oldtown of Utrecht, incl. the Oudegracht."""
    yield "Polygon ((136251.531 456118.126, 136620.128 456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 456118.126))"


@pytest.fixture
def database():
    db = DatabaseResource(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME
    )
    yield db


@pytest.fixture
def file_store(tmp_path):
    yield FileStoreResource(data_dir=str(tmp_path))


@pytest.fixture
def context(database, wkt_testarea, file_store, gdal):
    yield build_op_context(
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": [
                "gebouw",
            ],
        },
        resources={
            "gdal": gdal,
            "db_connection": database,
            "file_store": file_store,
        },
    )


def pytest_addoption(parser):
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--run-all",
        action="store_true",
        default=False,
        help="run all tests, including the ones that needs local builds of tools",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line(
        "markers", "needs_tools: mark test as needing local builds of tools"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-slow"):  # pragma: no cover
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--run-all"):  # pragma: no cover
        skip_needs_tools = pytest.mark.skip(reason="needs the --run-all option to run")
        for item in items:
            if "needs_tools" in item.keywords:
                item.add_marker(skip_needs_tools)


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)


@pytest.fixture(scope="session")
def laz_files_ahn3_dir(test_data_dir):
    yield test_data_dir / "reconstruction_input/pointcloud/AHN3/tiles_200m/"
