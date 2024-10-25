import os
import pickle
from pathlib import Path

import pytest
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.files import FileStoreResource
from dagster import AssetKey, IOManager, SourceAsset, build_op_context
import pandas as pd

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = os.getenv("BAG3D_PG_HOST")
PORT = os.getenv("BAG3D_PG_PORT")
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")

# update quadtree
og_quadtree = Path(LOCAL_DIR) / "quadtree.tsv"
export_dir = Path(LOCAL_DIR) / "reconstruction_input" / "3DBAG" / "export"
os.system(f"cp {og_quadtree} {export_dir}")


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)


@pytest.fixture(scope="session")
def input_data_dir(test_data_dir) -> Path:
    """Root directory path for test data"""
    return test_data_dir / "reconstruction_input"


@pytest.fixture(scope="session")
def fastssd_data_dir(test_data_dir) -> Path:
    """Root directory path for test data"""
    return test_data_dir / "integration_party_walls"


@pytest.fixture(scope="session")
def intermediate_data_dir(test_data_dir) -> Path:
    """Root directory path for test data"""
    return test_data_dir / "intermediate_data"


@pytest.fixture
def database():
    db = DatabaseResource(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME
    )
    yield db


@pytest.fixture
def context(database, input_data_dir, fastssd_data_dir):
    yield build_op_context(
        partition_key="10/564/624",
        resources={
            "db_connection": database,
            "file_store": FileStoreResource(data_dir=str(input_data_dir)),
            "file_store_fastssd": FileStoreResource(data_dir=str(fastssd_data_dir)),
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
def mock_party_walls_nl(intermediate_data_dir) -> pd.DataFrame:
    return pd.read_csv(intermediate_data_dir / "party_walls_nl.csv")


@pytest.fixture(scope="session")
def mock_features_file_index(intermediate_data_dir, fastssd_data_dir):
    data = pickle.load(open(intermediate_data_dir / "features_file_index.pkl", "rb"))
    for k, v in data.items():
        data[k] = Path(str(v).replace(str(v.parents[8]), str(fastssd_data_dir)))
    return data


@pytest.fixture(scope="session")
def mock_distribution_tiles_files_index(intermediate_data_dir, input_data_dir):
    data = pickle.load(
        open(intermediate_data_dir / "distribution_tiles_files_index.pkl", "rb")
    )
    for i, d in enumerate(data.paths_array):
        data.paths_array[i] = Path(
            str(d).replace(str(d.parents[6]), str(input_data_dir))
        )
    for k, v in data.export_results.items():
        cj_path = data.export_results[k].cityjson_path
        data.export_results[k].cityjson_path = Path(
            str(cj_path).replace(str(cj_path.parents[6]), str(input_data_dir))
        )
        gpkg_path = data.export_results[k].gpkg_path
        data.export_results[k].gpkg_path = Path(
            str(gpkg_path).replace(str(gpkg_path.parents[6]), str(input_data_dir))
        )
        # TODO: fix data.export_results[k].obj_paths
    return data


@pytest.fixture(scope="session")
def mock_asset_features_file_index(mock_features_file_index):
    class MockIOManager(IOManager):
        def load_input(self, context):
            return mock_features_file_index

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["party_walls", "features_file_index"]),
        io_manager_def=MockIOManager(),
    )


@pytest.fixture(scope="session")
def mock_asset_distribution_tiles_files_index(mock_distribution_tiles_files_index):
    class MockIOManager(IOManager):
        def load_input(self, context):
            return mock_distribution_tiles_files_index

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["party_walls", "distribution_tiles_files_index"]),
        io_manager_def=MockIOManager(),
    )
