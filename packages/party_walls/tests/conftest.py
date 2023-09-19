from pathlib import Path

import pytest

from bag3d.common.utils.files import bag3d_export_dir


@pytest.fixture(scope="session")
def root_data_dir() -> Path:
    """Root directory path for test data"""
    return Path("data").resolve()


@pytest.fixture(scope="session")
def input_data_dir(root_data_dir) -> Path:
    """Directory for input data"""
    return root_data_dir / "input"


@pytest.fixture(scope="session")
def export_dir_uncompressed(input_data_dir) -> Path:
    """3D BAG exported data before compression"""
    return input_data_dir / "export_uncompressed"


@pytest.fixture(scope="session")
def output_data_dir(root_data_dir) -> Path:
    """Directory for data generated during test runs"""
    return root_data_dir / "output"
