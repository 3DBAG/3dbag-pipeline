import os
from pathlib import Path

import pytest

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import GDALResource, ValidationResource
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA
from dagster import AssetKey, AssetSpec, IOManager, io_manager, build_op_context

pytest_plugins = ["bag3d.common.testing.conftest_plugin"]

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA", "")
HOST = os.getenv("BAG3D_PG_HOST", "")
PORT = int(os.getenv("BAG3D_PG_PORT", "5432"))
USER = os.getenv("BAG3D_PG_USER", "")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD", "")
DB_NAME = os.getenv("BAG3D_PG_DATABASE", "")


@pytest.fixture
def database():
    """Live database connection for tests that require a real database."""
    db = DatabaseResource(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME
    )
    yield db


class MockAssetIOManager(IOManager):
    """IO manager that returns pre-configured values for mock assets."""

    def __init__(self, values: dict):
        self._values = values

    def load_input(self, context):
        key = tuple(context.asset_key.path)
        if key not in self._values:
            raise KeyError(f"No mock value configured for asset {key}")
        return self._values[key]

    def handle_output(self, context, obj):
        # No-op for mock assets - they don't produce outputs
        pass


@io_manager
def mock_asset_io_manager(init_context):
    """Factory for creating mock IO managers with configured values."""
    values = init_context.resource_config.get("values", {})
    # Convert string keys back to tuples
    values = {tuple(k.split("/")): v for k, v in values.items()}
    return MockAssetIOManager(values)


@pytest.fixture(scope="function")
def wkt_testarea():
    """A small test area in the oldtown of Utrecht, incl. the Oudegracht."""
    yield "Polygon ((136251.531 456118.126, 136620.128 456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 456118.126))"


@pytest.fixture
def context_ahn():
    yield build_op_context(partition_key="01cz1")


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)


@pytest.fixture(scope="session")
def gdal():
    yield GDALResource(
        exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
        exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
        exe_sozip=os.getenv("EXE_PATH_SOZIP"),
    )


@pytest.fixture(scope="session")
def gdal_missing():
    yield GDALResource(
        exe_ogr2ogr="/does/not/exist/ogr2ogr",
        exe_ogrinfo="/does/not/exist/ogrinfo",
        exe_sozip="/does/not/exist/sozip",
    )


@pytest.fixture(scope="session")
def validation():
    yield ValidationResource(
        exe_val3dity=os.getenv("EXE_PATH_VAL3DITY"),
        exe_cjval=os.getenv("EXE_PATH_CJVAL"),
        exe_cjio=os.getenv("EXE_PATH_CJIO"),
    )


@pytest.fixture(scope="session")
def validation_missing():
    yield ValidationResource(
        exe_val3dity="/does/not/exist/val3dity",
        exe_cjval="/does/not/exist/cjval",
        exe_cjio="/does/not/exist/cjio",
    )


@pytest.fixture
def resources(database, file_store, gdal, validation):
    return {
        "gdal": gdal,
        "validation": validation,
        "computation_db": database,
        "file_store": file_store,
        "version": ReleaseVersionResource(version="test_version"),
        "specs": Specs3DBAGResource(),
    }


@pytest.fixture
def resources_missing(database, file_store, gdal_missing, validation_missing):
    return {
        "gdal": gdal_missing,
        "validation": validation_missing,
        "computation_db": database,
        "file_store": file_store,
        "version": ReleaseVersionResource(version="test_version"),
        "specs": Specs3DBAGResource(),
    }


@pytest.fixture
def resources_ahn(database, file_store):
    return {
        "computation_db": database,
        "file_store": file_store,
        "version": ReleaseVersionResource(version="test_version"),
        "specs": Specs3DBAGResource(),
    }


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
def tile_index_ahn_fix():
    yield {
        "01cz1": {
            "AHN3_LAZ": "https://basisdata.nl/hwh-ahn/AHN3/LAZ/C_01CZ1.LAZ",
            "AHN4_LAZ": "https://basisdata.nl/hwh-ahn/ahn4/01_LAZ/C_01CZ1.LAZ",
            "AHN5_LAZ": "https://basisdata.nl/hwh-ahn/AHN5/01_LAZ//2023_C_01CZ1.LAZ",
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
    return AssetSpec(
        key=AssetKey(["input", "reconstruction_input"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_tiles():
    return AssetSpec(
        key=AssetKey(["input", "tiles"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_index():
    return AssetSpec(
        key=AssetKey(["input", "index"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_metadata_ahn3_index():
    return AssetSpec(
        key=AssetKey(["ahn", "metadata_ahn3_index"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_metadata_ahn4_index():
    return AssetSpec(
        key=AssetKey(["ahn", "metadata_ahn4_index"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_metadata_ahn5_index():
    return AssetSpec(
        key=AssetKey(["ahn", "metadata_ahn5_index"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_values():
    """Values to be returned by the mock IO manager for each asset key."""
    return {
        "input/reconstruction_input": PostgresTableIdentifier(
            RECONSTRUCTION_INPUT_SCHEMA, "reconstruction_input"
        ),
        "input/tiles": PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "tiles"),
        "input/index": PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "index"),
        "ahn/metadata_ahn3_index": PostgresTableIdentifier("ahn", "metadata_ahn3"),
        "ahn/metadata_ahn4_index": PostgresTableIdentifier("ahn", "metadata_ahn4"),
        "ahn/metadata_ahn5_index": PostgresTableIdentifier("ahn", "metadata_ahn5"),
    }


@pytest.fixture(scope="session")
def configured_mock_asset_io_manager(mock_asset_values):
    """Configured IO manager resource with pre-set values for mock assets."""
    return mock_asset_io_manager.configured({"values": mock_asset_values})
