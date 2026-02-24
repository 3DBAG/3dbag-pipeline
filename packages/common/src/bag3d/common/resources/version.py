from subprocess import run, TimeoutExpired, CalledProcessError
from typing import Annotated, Optional, Dict
import random
import string

from dagster import ConfigurableResource, get_dagster_logger
from pydantic import BeforeValidator

logger = get_dagster_logger()


def _make_release_version(v: str | None) -> str:
    return v or "".join(random.choice(string.ascii_letters) for _ in range(8))


class ReleaseVersionResource(ConfigurableResource):
    """A resource for setting up the version release."""

    version: Annotated[str, BeforeValidator(_make_release_version)] = None  # type: ignore[assignment]


class ToolVersionsResource(ConfigurableResource):
    """Extracts and caches tool versions for use in asset code_version.

    This resource is instantiated at module import time (not configure_at_launch)
    to make versions available for code_version decorator parameters.
    """

    # Tool executable paths (same env vars as other resources)
    exe_tyler: Optional[str] = None
    exe_tyler_db: Optional[str] = None
    exe_tyler_multiformat: Optional[str] = None
    exe_roofer: Optional[str] = None
    exe_ogr2ogr: Optional[str] = None
    exe_pdal: Optional[str] = None
    exe_lasindex: Optional[str] = None
    exe_geof: Optional[str] = None

    # Cached versions
    _version_cache: Dict[str, str] = {}

    def _extract_version(
        self, exe_path: Optional[str], version_flag: str = "--version"
    ) -> str:
        """Extract version by running tool with version flag."""
        if not exe_path:
            return "unknown"

        try:
            result = run(
                [exe_path, version_flag],
                capture_output=True,
                text=True,
                timeout=5,
            )
            # Return first line of output, sanitized
            return result.stdout.strip().split("\n")[0].replace(",", " ")
        except (TimeoutExpired, CalledProcessError, FileNotFoundError) as e:
            logger.warning(f"Failed to extract version for {exe_path}: {e}")
            return "unknown"

    def get_version(self, tool_name: str) -> str:
        """Get version for a tool, with caching."""
        if tool_name in self._version_cache:
            return self._version_cache[tool_name]

        # Extract version based on tool name
        version = "unknown"
        if tool_name == "tyler":
            version = self._extract_version(self.exe_tyler)
        elif tool_name == "tyler-db":
            version = self._extract_version(self.exe_tyler_db)
        elif tool_name == "tyler-multiformat":
            version = self._extract_version(self.exe_tyler_multiformat)
        elif tool_name == "roofer":
            version = self._extract_version(self.exe_roofer)
        elif tool_name == "ogr2ogr":
            version = self._extract_version(self.exe_ogr2ogr)
        elif tool_name == "pdal":
            version = self._extract_version(self.exe_pdal)
        elif tool_name == "lasindex":
            version = self._extract_version(self.exe_lasindex, "-version")
        elif tool_name == "geof":
            version = self._extract_version(self.exe_geof, "--list-plugins")

        self._version_cache[tool_name] = version
        return version
