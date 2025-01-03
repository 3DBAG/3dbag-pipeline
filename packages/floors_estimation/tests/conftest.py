import os
import pickle
from pathlib import Path

import pytest
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.files import FileStoreResource
from dagster import build_op_context

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = os.getenv("BAG3D_PG_HOST")
PORT = os.getenv("BAG3D_PG_PORT")
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")


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
    return test_data_dir / "integration_floors_estimation"


@pytest.fixture(scope="session")
def intermediate_data_dir(test_data_dir) -> Path:
    """Root directory path for test data"""
    return test_data_dir / "intermediate_data"


@pytest.fixture(scope="session")
def model(test_data_dir) -> Path:
    """Directory for the floors estimation model"""
    return test_data_dir / "model" / "pipeline_model1_gbr_untuned.joblib"


@pytest.fixture
def database():
    db = DatabaseResource(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME
    )
    yield db


@pytest.fixture
def context(database, input_data_dir, model, fastssd_data_dir):
    yield build_op_context(
        partition_key="10/564/624",
        resources={
            "db_connection": database,
            "file_store": FileStoreResource(data_dir=str(input_data_dir)),
            "file_store_fastssd": FileStoreResource(data_dir=str(fastssd_data_dir)),
            "model_store": model,
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
def mock_preprocessed_features(intermediate_data_dir):
    return pickle.load(open(intermediate_data_dir / "preprocessed_features.pkl", "rb"))


@pytest.fixture(scope="session")
def mock_features_file_index(intermediate_data_dir, fastssd_data_dir):
    data = pickle.load(
        open(
            intermediate_data_dir / "features_file_index_floors_estimation.pkl",
            "rb",
        )
    )
    for k, v in data.items():
        data[k] = Path(str(v).replace(str(v.parents[5]), str(fastssd_data_dir)))
    return data


@pytest.fixture(scope="session")
def mock_inferenced_floors(intermediate_data_dir):
    return pickle.load(open(intermediate_data_dir / "inferenced_floors.pkl", "rb"))
