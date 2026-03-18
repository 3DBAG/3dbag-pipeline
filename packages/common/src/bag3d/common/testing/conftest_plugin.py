import pytest
from unittest.mock import MagicMock

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.database import DatabaseResource


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
        help="run all tests, including the ones that need local builds of tools",
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
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--run-all"):
        skip_needs_tools = pytest.mark.skip(reason="needs the --run-all option to run")
        for item in items:
            if "needs_tools" in item.keywords:
                item.add_marker(skip_needs_tools)

    if not config.getoption("--run-deploy"):
        skip_needs_deploy = pytest.mark.skip(
            reason="needs the --run-deploy option to run"
        )
        for item in items:
            if "needs_deploy" in item.keywords:
                item.add_marker(skip_needs_deploy)


@pytest.fixture
def database():
    return MagicMock(spec=DatabaseResource)


@pytest.fixture
def file_store(tmp_path):
    return FileStoreResource(root_dir=str(tmp_path))


@pytest.fixture
def pointcloud_store(tmp_path):
    return FileStoreResource(root_dir=str(tmp_path / "pointcloud"))
