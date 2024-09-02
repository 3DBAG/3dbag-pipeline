import os
from pathlib import Path

import pytest
from bag3d.common.resources.database import DatabaseConnection
from bag3d.common.resources.files import file_store
from dagster import build_op_context
from pandas import DataFrame

LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = "localhost"
PORT = os.getenv("BAG3D_PG_PORT")
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)


@pytest.fixture(scope="session")
def root_data_dir(test_data_dir) -> Path:
    """Root directory path for test data"""
    return test_data_dir / "reconstruction_data"


@pytest.fixture(scope="session")
def intermediate_data_dir(test_data_dir) -> Path:
    """Root directory path for test data"""
    return test_data_dir / "intermediate_data"


@pytest.fixture(scope="session")
def input_data_dir(root_data_dir) -> Path:
    """Directory for input data"""
    return root_data_dir / "input"


@pytest.fixture(scope="session")
def model_dir(test_data_dir) -> Path:
    """Directory for the floors estimation model"""
    return test_data_dir / "model" / "pipeline_model1_gbr_untuned.joblib"


@pytest.fixture(scope="session")
def export_dir_uncompressed(input_data_dir) -> Path:
    """3D BAG exported data before compression"""
    return input_data_dir / "export_uncompressed"


@pytest.fixture(scope="function")
def database():
    db = DatabaseConnection(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB_NAME
    )
    yield db


@pytest.fixture
def context(database, export_dir_uncompressed, input_data_dir, model_dir):
    yield build_op_context(
        partition_key="10/564/624",
        resources={
            "db_connection": database,
            "file_store": file_store.configured(
                {
                    "data_dir": str(export_dir_uncompressed),
                }
            ),
            "file_store_fastssd": file_store.configured(
                {
                    "data_dir": str(input_data_dir),
                }
            ),
            "model_store": model_dir,
        },
    )


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
def mock_preprocessed_features(intermediate_data_dir) -> DataFrame:
    class MockIOManager(IOManager):
        def load_input(self, context):
            return pickle.load(
                open(intermediate_data_dir / "preprocessed_features.pkl", "rb")
            )

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["floors_estimation", "preprocessed_features"]),
        io_manager_def=MockIOManager(),
    )


@pytest.fixture(scope="session")
def mock_features_file_index(intermediate_data_dir, input_data_dir) -> dict[str, Path]:
    class MockIOManager(IOManager):
        def load_input(self, context):
            data = pickle.load(
                open(
                    intermediate_data_dir / "features_file_index_floors_estimation.pkl",
                    "rb",
                )
            )
            for k, v in data.items():
                data[k] = Path(str(v).replace(str(v.parents[5]), str(input_data_dir)))
            return data

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["floors_estimation", "features_file_index"]),
        io_manager_def=MockIOManager(),
    )


@pytest.fixture(scope="session")
def mock_inferenced_floors(intermediate_data_dir) -> DataFrame:
    class MockIOManager(IOManager):
        def load_input(self, context):
            return pickle.load(
                open(intermediate_data_dir / "inferenced_floors.pkl", "rb")
            )

        def handle_output(self, context, obj):  # pragma: no cover
            raise NotImplementedError()

    return SourceAsset(
        key=AssetKey(["floors_estimation", "inferenced_floors"]),
        io_manager_def=MockIOManager(),
    )
