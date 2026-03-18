import os  # noqa: E402

import pytest  # noqa: E402
from bag3d.common.resources.version import ReleaseVersionResource  # noqa: E402

pytest_plugins = ["bag3d.common.testing.conftest_plugin"]

VERSION = "test_version"

# Ensure partition definitions can read the version from environment
os.environ["BAG3D_RELEASE_VERSION"] = VERSION


@pytest.fixture
def version():
    yield ReleaseVersionResource(version=VERSION)
