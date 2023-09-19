from pathlib import Path

import pytest

from bag3d.common.utils.files import bag3d_export_dir


@pytest.fixture(scope="session")
def root_data_dir():
    """Root directory path for test data"""
    return "/data"

@pytest.fixture(scope="session")
def export_dir(root_data_dir):
    """3D BAG export data directory path"""
    return bag3d_export_dir(root_data_dir)
