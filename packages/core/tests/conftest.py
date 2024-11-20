import os
from pathlib import Path

import pytest
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import (
    DOCKER_GDAL_IMAGE,
    GDALResource,
    DockerConfig,
)

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA
from dagster import AssetKey, IOManager, SourceAsset, build_op_context

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = os.getenv("BAG3D_PG_HOST")
PORT = os.getenv("BAG3D_PG_PORT")
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")


@pytest.fixture(scope="session")
def docker_config():
    yield DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp")


@pytest.fixture(scope="session")
def gdal(docker_config):
    exe_ogr2ogr = os.getenv("EXE_PATH_OGR2OGR")
    exe_ogrinfo = os.getenv("EXE_PATH_OGRINFO")
    exe_sozip = os.getenv("EXE_PATH_SOZIP")
    if exe_ogr2ogr is None and exe_ogrinfo is None and exe_sozip is None:
        yield GDALResource(docker_cfg=docker_config)
    else:
        yield GDALResource(
            exe_ogr2ogr=exe_ogr2ogr,
            exe_ogrinfo=exe_ogrinfo,
            exe_sozip=exe_sozip,
        )


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
        partition_key="01cz1",
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
            "version": "test_version",
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
def md5_ahn3_fix():
    yield {"C_01CZ1.LAZ": "063b23d038f97576d279fb7d8a1481ad"}


@pytest.fixture(scope="session")
def md5_ahn4_fix():
    yield {"C_01CZ1.LAZ": "56c731a1814dd73c79a0a5347f8a04c7"}


@pytest.fixture(scope="session")
def sha256_ahn5_fix():
    yield {
        "2023_C_01CZ1.LAZ": "067541da253de88eef78c580a1ff6396c7ec3e3833cc0843a2fac4270b625611"
    }


@pytest.fixture(scope="session")
def tile_index_pdok_fix():
    yield {
        "01cz1": {
            "AHN3_LAZ": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/LAZ/C_01CZ1.LAZ",
            "AHN4_LAZ": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/01_LAZ/C_01CZ1.LAZ",
            "AHN5_LAZ": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN5/01_LAZ/2023_C_01CZ1.LAZ",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [140000.0001043251, 600000.0005214083],
                        [140000.00010435135, 606250.0005244487],
                        [145000.0001086861, 606250.0005244645],
                        [145000.0001086705, 600000.0005214227],
                        [140000.0001043251, 600000.0005214083],
                    ]
                ],
                "geometry_name": "shape",
            },
        }
    }


@pytest.fixture(scope="session")
def mock_asset_reconstruction_input():
    class MockIOManager(IOManager):
        def load_input(self, context):
            new_table = PostgresTableIdentifier(
                RECONSTRUCTION_INPUT_SCHEMA, "reconstruction_input"
            )
            return new_table

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["input", "reconstruction_input"]),
        io_manager_def=MockIOManager(),
    )


@pytest.fixture(scope="session")
def mock_asset_tiles():
    class MockIOManager(IOManager):
        def load_input(self, context):
            new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "tiles")
            return new_table

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["input", "tiles"]),
        io_manager_def=MockIOManager(),
    )


@pytest.fixture(scope="session")
def mock_asset_index():
    class MockIOManager(IOManager):
        def load_input(self, context):
            new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "index")
            return new_table

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["input", "index"]),
        io_manager_def=MockIOManager(),
    )


@pytest.fixture(scope="session")
def mock_asset_regular_grid_200m():
    class MockIOManager(IOManager):
        def load_input(self, context):
            new_table = PostgresTableIdentifier("ahn", "regular_grid_200m")
            return new_table

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["ahn", "regular_grid_200m"]),
        io_manager_def=MockIOManager(),
    )
