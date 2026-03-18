from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.files import FileStoreResource


@pytest.fixture
def database():
    return MagicMock(spec=DatabaseResource)


@pytest.fixture
def file_store(tmp_path):
    return FileStoreResource(root_dir=str(tmp_path))


@pytest.fixture
def pointcloud_store(tmp_path):
    return FileStoreResource(root_dir=str(tmp_path / "pointcloud"))


@pytest.fixture
def test_data_dir(tmp_path) -> Path:
    return tmp_path / "test_data"
