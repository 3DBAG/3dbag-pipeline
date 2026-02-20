import os
from pathlib import Path

import pytest

from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import (
    GDALResource,
    ValidationResource,
)
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.common.resources.server_transfer import ServerTransferResource


from bag3d.common.resources.files import FileStoreResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA
from dagster import AssetKey, AssetSpec, IOManager, io_manager, build_op_context

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = os.getenv("BAG3D_PG_HOST")
PORT = int(os.getenv("BAG3D_PG_PORT"))
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")


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


@pytest.fixture(scope="session")
def deployment_server():
    """Connection to the dockerized deployment setup.
    The dockerized deployment setup is in the 3dbag-admin repo and it needs to be
    managed manually, similar to the 3dbag-pipeline docker setup.
    These credentials provide access to the ``deployment-server`` service of the
    deployment setup.
    """
    server = ServerTransferResource(
        host="3dbag.docker.internal",
        port=2222,
        user="deploy",
        password="deploy",
        target_dir="/data/3DBAG",
        public_dir="/data/3DBAG/public",
    )

    yield server


@pytest.fixture(scope="session")
def godzilla_server(deployment_server):
    yield deployment_server


@pytest.fixture(scope="session")
def podzilla_server():
    yield ServerTransferResource(
        host="3dbag.docker.internal",
        port=2222,
        user="deploy",
        password="deploy",
        target_dir="/tmp",
        public_dir="/tmp/podzilla_public",
    )


@pytest.fixture(scope="session")
def gdal():
    exe_ogr2ogr = os.getenv("EXE_PATH_OGR2OGR")
    exe_ogrinfo = os.getenv("EXE_PATH_OGRINFO")
    exe_sozip = os.getenv("EXE_PATH_SOZIP")
    yield GDALResource(
        exe_ogr2ogr=exe_ogr2ogr,
        exe_ogrinfo=exe_ogrinfo,
        exe_sozip=exe_sozip,
    )


@pytest.fixture(scope="session")
def gdal_missing():
    yield GDALResource(docker_image="nonexistent:missing")


@pytest.fixture(scope="session")
def validation():
    exe_val3dity = os.getenv("EXE_PATH_VAL3DITY")
    exe_cjval = os.getenv("EXE_PATH_CJVAL")
    exe_cjio = os.getenv("EXE_PATH_CJIO")
    yield ValidationResource(
        exe_val3dity=exe_val3dity,
        exe_cjval=exe_cjval,
        exe_cjio=exe_cjio,
    )


@pytest.fixture(scope="session")
def validation_missing():
    yield ValidationResource(docker_image="nonexistent:missing")


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
def context(
    wkt_testarea,
):
    yield build_op_context(
        partition_key="01cz1",
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": [
                "gebouw",
            ],
            "parallel": True,
        },
    )


@pytest.fixture
def resources(
    database,
    file_store,
    gdal,
    validation,
    godzilla_server,
    podzilla_server,
):
    return {
        "gdal": gdal,
        "validation": validation,
        "db_connection": database,
        "file_store": file_store,
        "version": ReleaseVersionResource("test_version"),
        "godzilla_server": godzilla_server,
        "podzilla_server": podzilla_server,
        "specs": Specs3DBAGResource(),
    }


@pytest.fixture
def context_ahn():
    yield build_op_context(partition_key="01cz1")


@pytest.fixture
def resources_ahn(
    database,
    file_store,
    gdal,
    validation,
    godzilla_server,
    podzilla_server,
):
    return {
        "gdal": gdal,
        "validation": validation,
        "db_connection": database,
        "file_store": file_store,
        "version": ReleaseVersionResource("test_version"),
        "godzilla_server": godzilla_server,
        "podzilla_server": podzilla_server,
        "specs": Specs3DBAGResource(),
    }


@pytest.fixture
def resources_missing(database, file_store, gdal_missing, validation_missing):
    return {
        "gdal": gdal_missing,
        "validation": validation_missing,
        "db_connection": database,
        "file_store": file_store,
        "version": ReleaseVersionResource("test_version"),
    }


@pytest.fixture
def context_missing(
    wkt_testarea,
):
    yield build_op_context(
        partition_key="01cz1",
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": [
                "gebouw",
            ],
            "parallel": True,
        },
    )


@pytest.fixture
def context_top10nl(wkt_testarea):
    yield build_op_context(
        partition_key="01cz1",
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": [
                "gebouw",
            ],
        },
    )


@pytest.fixture
def context_bgt(wkt_testarea):
    yield build_op_context(
        partition_key="01cz1",
        op_config={
            "geofilter": wkt_testarea,
            "featuretypes": [
                "pand",
            ],
        },
    )


def pytest_addoption(parser):
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--run-deploy",
        action="store_true",
        default=False,
        help="run deployment tests that require the dockerized deployment setup",
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
    config.addinivalue_line(
        "markers", "needs_deploy: mark test as needing the dockerized deployment setup"
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

    if not config.getoption("--run-deploy"):  # pragma: no cover
        skip_needs_deploy = pytest.mark.skip(
            reason="needs the --run-deploy option to run"
        )
        for item in items:
            if "needs_deploy" in item.keywords:
                item.add_marker(skip_needs_deploy)


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)


@pytest.fixture(scope="session")
def core_integration_test_dir(test_data_dir):
    yield test_data_dir / "integration_core"


@pytest.fixture(scope="session")
def core_file_store_fastssd(core_integration_test_dir) -> Path:
    """Root directory path for test data"""
    return core_integration_test_dir / "file_store_fastssd"


@pytest.fixture(scope="session")
def core_file_store(core_integration_test_dir) -> Path:
    """Root directory path for test data"""
    return core_integration_test_dir / "file_store"


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
def mock_asset_compressed_tiles():
    return AssetSpec(
        key=AssetKey(["export", "compressed_tiles"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_compressed_tiles_validation(test_data_dir):
    return AssetSpec(
        key=AssetKey(["export", "compressed_tiles_validation"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_export_index(test_data_dir):
    return AssetSpec(
        key=AssetKey(["export", "export_index"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_geopackage_nl(test_data_dir):
    return AssetSpec(
        key=AssetKey(["export", "geopackage_nl"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_metadata(test_data_dir):
    return AssetSpec(
        key=AssetKey(["export", "metadata"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_reconstruction_output_3dtiles_lod12_nl(test_data_dir):
    return AssetSpec(
        key=AssetKey(["export", "reconstruction_output_3dtiles_lod12_nl"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_reconstruction_output_3dtiles_lod13_nl(test_data_dir):
    return AssetSpec(
        key=AssetKey(["export", "reconstruction_output_3dtiles_lod13_nl"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_reconstruction_output_3dtiles_lod22_nl(test_data_dir):
    return AssetSpec(
        key=AssetKey(["export", "reconstruction_output_3dtiles_lod22_nl"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_reconstruction_output_multitiles_nl(test_data_dir):
    return AssetSpec(
        key=AssetKey(["export", "reconstruction_output_multitiles_nl"]),
        metadata={"dagster/io_manager_key": "mock_asset_io_manager"},
    )


@pytest.fixture(scope="session")
def mock_asset_values(test_data_dir):
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
        "export/compressed_tiles": None,
        "export/compressed_tiles_validation": (
            test_data_dir
            / "integration_deploy_release"
            / "3DBAG"
            / "export_test_version"
            / "validate_compressed_files.csv"
        ),
        "export/export_index": (
            test_data_dir
            / "integration_deploy_release"
            / "3DBAG"
            / "export_test_version"
            / "export_index.csv"
        ),
        "export/geopackage_nl": (
            test_data_dir
            / "integration_deploy_release"
            / "3DBAG"
            / "export_test_version"
            / "3dbag_nl.gpkg.zip"
        ),
        "export/metadata": (
            test_data_dir
            / "integration_deploy_release"
            / "3DBAG"
            / "export_test_version"
            / "metadata.json"
        ),
        "export/reconstruction_output_3dtiles_lod12_nl": (
            test_data_dir
            / "integration_deploy_release"
            / "3DBAG"
            / "export_test_version"
            / "cesium3dtiles"
            / "lod12"
        ),
        "export/reconstruction_output_3dtiles_lod13_nl": (
            test_data_dir
            / "integration_deploy_release"
            / "3DBAG"
            / "export_test_version"
            / "cesium3dtiles"
            / "lod13"
        ),
        "export/reconstruction_output_3dtiles_lod22_nl": (
            test_data_dir
            / "integration_deploy_release"
            / "3DBAG"
            / "export_test_version"
            / "cesium3dtiles"
            / "lod22"
        ),
        "export/reconstruction_output_multitiles_nl": (
            test_data_dir
            / "integration_deploy_release"
            / "3DBAG"
            / "export_test_version"
            / "tiles"
        ),
    }


@pytest.fixture(scope="session")
def configured_mock_asset_io_manager(mock_asset_values):
    """Configured IO manager resource with pre-set values for mock assets."""
    return mock_asset_io_manager.configured({"values": mock_asset_values})
