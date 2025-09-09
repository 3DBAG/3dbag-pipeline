import os
from pathlib import Path, PosixPath

import pytest
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import VersionResource
from bag3d.common.types import ExportResult
from bag3d.party_walls.assets.party_walls import (
    TilesFilesIndex,
)
from dagster import build_op_context
import pandas as pd
from shapely import STRtree, from_wkt
import numpy as np


LOCAL_DIR = os.getenv("BAG3D_TEST_DATA")
HOST = os.getenv("BAG3D_PG_HOST")
PORT = os.getenv("BAG3D_PG_PORT")
USER = os.getenv("BAG3D_PG_USER")
PASSWORD = os.getenv("BAG3D_PG_PASSWORD")
DB_NAME = os.getenv("BAG3D_PG_DATABASE")
VERSION = "test_version"


@pytest.fixture(scope="session")
def test_data_dir():
    yield Path(LOCAL_DIR)


@pytest.fixture(scope="session")
def party_walls_integration_test_dir(test_data_dir):
    yield test_data_dir / "integration_party_walls"


@pytest.fixture(scope="session")
def party_walls_file_store_fastssd(party_walls_integration_test_dir) -> Path:
    """Root directory path for test data"""
    return party_walls_integration_test_dir / "file_store_fastssd"


@pytest.fixture(scope="session")
def party_walls_file_store(party_walls_integration_test_dir) -> Path:
    """Root directory path for test data"""
    return party_walls_integration_test_dir / "file_store"


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
def context(database, party_walls_file_store, party_walls_file_store_fastssd):
    yield build_op_context(
        partition_key="0/0/0",
        resources={
            "db_connection": database,
            "file_store": FileStoreResource(data_dir=str(party_walls_file_store)),
            "file_store_fastssd": FileStoreResource(
                data_dir=str(party_walls_file_store_fastssd)
            ),
            "version": VersionResource(VERSION),
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
def mock_features_file_index(party_walls_file_store_fastssd):
    return {
        "NL.IMBAG.Pand.0307100000308298": party_walls_file_store_fastssd
        / "3DBAG/crop_reconstruct/10/564/624/objects/NL.IMBAG.Pand.0307100000308298/reconstruct/NL.IMBAG.Pand.0307100000308298.city.jsonl",
        "NL.IMBAG.Pand.0307100000368987": party_walls_file_store_fastssd
        / "3DBAG/crop_reconstruct/10/564/624/objects/NL.IMBAG.Pand.0307100000368987/reconstruct/NL.IMBAG.Pand.0307100000368987.city.jsonl",
        "NL.IMBAG.Pand.0307100000547663": party_walls_file_store_fastssd
        / "3DBAG/crop_reconstruct/10/564/624/objects/NL.IMBAG.Pand.0307100000547663/reconstruct/NL.IMBAG.Pand.0307100000547663.city.jsonl",
        "NL.IMBAG.Pand.0307100000536600": party_walls_file_store_fastssd
        / "3DBAG/crop_reconstruct/10/564/624/objects/NL.IMBAG.Pand.0307100000536600/reconstruct/NL.IMBAG.Pand.0307100000536600.city.jsonl",
        "NL.IMBAG.Pand.0307100000313420": party_walls_file_store_fastssd
        / "3DBAG/crop_reconstruct/10/564/624/objects/NL.IMBAG.Pand.0307100000313420/reconstruct/NL.IMBAG.Pand.0307100000313420.city.jsonl",
        "NL.IMBAG.Pand.0307100000332591": party_walls_file_store_fastssd
        / "3DBAG/crop_reconstruct/10/564/624/objects/NL.IMBAG.Pand.0307100000332591/reconstruct/NL.IMBAG.Pand.0307100000332591.city.jsonl",
    }


@pytest.fixture(scope="session")
def mock_distribution_tiles_files_index(party_walls_file_store):
    export_results = {
        "0/0/0": ExportResult(
            tile_id="0/0/0",
            cityjson_path=PosixPath(
                f"{party_walls_file_store}/3DBAG/export_test_version/tiles/0/0/0/0-0-0.city.json"
            ),
            gpkg_path=PosixPath(
                f"{party_walls_file_store}/3DBAG/export_test_version/tiles/0/0/0/0-0-0.gpkg"
            ),
            obj_paths=(
                PosixPath(
                    f"{party_walls_file_store}/3DBAG/export_test_version/tiles/0/0/0/0-0-0-LoD13-3D.obj"
                ),
                PosixPath(
                    f"{party_walls_file_store}/3DBAG/export_test_version/tiles/0/0/0/0-0-0-LoD12-3D.obj"
                ),
                PosixPath(
                    f"{party_walls_file_store}/3DBAG/export_test_version/tiles/0/0/0/0-0-0-LoD22-3D.obj"
                ),
            ),
            wkt="POLYGON((154565.241 462855.414, 155565.241 462855.414, 155565.241 463855.414, 154565.241 463855.414, 154565.241 462855.414))",
        )
    }
    tree = STRtree(tuple(from_wkt(t.wkt) for t in export_results.values()))
    paths_array = np.array(tuple(t.cityjson_path for t in export_results.values()))
    return TilesFilesIndex(
        export_results=export_results, tree=tree, paths_array=paths_array
    )
