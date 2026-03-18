"""Read tool metadata from 3dbag-manifest.json."""

import json
import os
from pathlib import Path

_manifest_cache: dict | None = None


def _find_manifest() -> Path:
    """Find the manifest file.

    Search order:
    1. BAG3D_MANIFEST_PATH environment variable
    2. Docker path /opt/3dbag-pipeline/3dbag-manifest.json
    3. Walk up from this file to find repo root
    """
    env_path = os.environ.get("BAG3D_MANIFEST_PATH")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p

    docker_path = Path("/opt/3dbag-pipeline/3dbag-manifest.json")
    if docker_path.is_file():
        return docker_path

    current = Path(__file__).resolve().parent
    for parent in [current, *current.parents]:
        candidate = parent / "3dbag-manifest.json"
        if candidate.is_file():
            return candidate

    raise FileNotFoundError(
        "Cannot find 3dbag-manifest.json. "
        "Set BAG3D_MANIFEST_PATH or run from the repository root."
    )


def load_manifest() -> dict:
    """Parse and return the full manifest, cached after first call."""
    global _manifest_cache
    if _manifest_cache is None:
        path = _find_manifest()
        _manifest_cache = json.loads(path.read_text())
    assert _manifest_cache is not None
    return _manifest_cache


def get_tool_metadata(tool_name: str) -> dict:
    """Return a single tool's entry from the manifest."""
    manifest = load_manifest()
    tools = manifest.get("tools", {})
    if tool_name not in tools:
        raise KeyError(f"Tool '{tool_name}' not found in manifest")
    return tools[tool_name]
