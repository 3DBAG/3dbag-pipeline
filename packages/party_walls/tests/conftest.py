from pathlib import Path

import pytest

from bag3d.common.utils.files import bag3d_export_dir


@pytest.fixture(scope="session")
def root_data_dir() -> Path:
    """Root directory path for test data"""
    return Path("data").resolve()

@pytest.fixture(scope="session")
def export_dir_uncompressed(root_data_dir):
    """3D BAG exported data before compression"""
    return root_data_dir / "input" / "export_uncompressed"
