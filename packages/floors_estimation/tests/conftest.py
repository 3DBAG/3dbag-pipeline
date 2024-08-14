import os
from pathlib import Path

import pytest
from bag3d.common.resources.database import DatabaseConnection
from bag3d.common.resources.files import file_store
from dagster import build_op_context

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
def model_dir(root_data_dir) -> Path:
    """Directory for the floors estimation model"""
    return root_data_dir / "model" / "pipeline_model1_gbr_untuned.joblib"


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
