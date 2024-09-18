import os
from pathlib import Path

import pytest
from bag3d.common.resources.database import DatabaseConnection
from bag3d.common.resources.executables import (
    DOCKER_GDAL_IMAGE,
    GDALResource,
    DockerConfig,
)

from bag3d.common.resources.files import file_store
from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA
from dagster import AssetKey, IOManager, SourceAsset, build_op_context

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = "localhost"
PORT = os.getenv("BAG3D_PG_PORT")
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")


@pytest.fixture(scope="session")
def gdal():
    return GDALResource(
        docker_cfg=DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp")
    )


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
def context(database, wkt_testarea, tmp_path, gdal):
    yield build_op_context(
        partition_key="01cz1",
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": [
                "gebouw",
            ],
        },
        resources={
            "gdal": gdal.app,
            "db_connection": database,
            "file_store": file_store.configured(
                {
                    "data_dir": str(tmp_path),
                }
            ),
        },
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
    else:  # pragma: no cover
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)


@pytest.fixture(scope="session")
def md5_pdok_ahn3_fix():
    yield {"C_01CZ1.LAZ": "063b23d038f97576d279fb7d8a1481ad"}


@pytest.fixture(scope="session")
def md5_pdok_ahn4_fix():
    yield {"C_01CZ1.LAZ": "56c731a1814dd73c79a0a5347f8a04c7"}


@pytest.fixture(scope="session")
def tile_index_ahn3_pdok_fix():
    yield {
        "01cz1": {
            "type": "Feature",
            "id": 1706,
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [141000, 600000],
                        [141000, 602500],
                        [145000, 602500],
                        [145000, 600000],
                        [141000, 600000],
                    ]
                ],
            },
            "properties": {
                "OBJECTID": 1706,
                "CenterX": 143000,
                "CenterY": 601250,
                "Shape_Leng": 13000,
                "ahn2_05m_i": "http://geodata.nationaalgeoregister.nl/ahn2/extract/ahn2_05m_int/i01cz1.tif.zip",
                "ahn2_05m_n": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/DTM_50cm/i01cz1.tif.zip",
                "ahn2_05m_r": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/DSM_50cm/r01cz1.tif.zip",
                "ahn2_5m": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/DTM_5m/ahn2_5_01cz1.tif.zip",
                "ahn2_LAZ_g": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/ahn2_laz_units_gefilterd/g01cz1.laz",
                "ahn2_LAZ_u": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/ahn2_laz_units_uitgefilterd/u01cz1.laz",
                "Kaartblad": "01cz1",
                "ahn1_5m": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN1/DTM_5m/01cz1.tif.zip",
                "ahn1_LAZ_g": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN1/ahn1_gefilterd/01cz1.laz.zip",
                "ahn1_LAZ_u": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN1/ahn1_uitgefilterd/u01cz1.laz.zip",
                "AHN3_05m_DSM": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/DSM_50cm/R_01CZ1.zip",
                "AHN3_05m_DTM": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/DTM_50cm/M_01CZ1.zip",
                "AHN3_5m_DSM": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/DSM_5m/R5_01CZ1.zip",
                "AHN3_5m_DTM": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/DTM_5m/M5_01CZ1.zip",
                "AHN3_LAZ": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/LAZ/C_01CZ1.LAZ",
                "AHN_Beschikbaar": "AHN1, AHN2, AHN3",
            },
        }
    }


@pytest.fixture(scope="session")
def tile_index_ahn4_pdok_fix():
    yield {
        "01cz1": {
            "type": "Feature",
            "id": 1,
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [140000, 600000],
                        [140000, 606250],
                        [145000, 606250],
                        [145000, 600000],
                        [140000, 600000],
                    ]
                ],
            },
            "properties": {
                "OBJECTID": 1,
                "Name": "01CZ1",
                "AHN4_DTM_05m": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/02a_DTM_0.5m/M_01CZ1.zip",
                "AHN4_DTM_5m": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/02b_DTM_5m/M5_01CZ1.zip",
                "AHN4_DSM_05m": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/03a_DSM_0.5m/R_01CZ1.zip",
                "AHN4_DSM_5m": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/03b_DSM_5m/R5_01CZ1.zip",
                "AHN4_LAZ": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/01_LAZ/C_01CZ1.LAZ",
                "Shape__Area": 31250000,
                "Shape__Length": 22500,
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
