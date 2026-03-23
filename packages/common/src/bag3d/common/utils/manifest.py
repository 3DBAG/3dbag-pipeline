"""Read tool metadata from 3dbag-manifest.json."""

import json
import os
from pathlib import Path

_manifest_cache: dict | None = None
_MANIFEST_FILENAME = "3dbag-manifest.json"
_DOCKER_MANIFEST_PATH = Path("/opt/3dbag-pipeline") / _MANIFEST_FILENAME


def _walk_manifest_parents(start: Path) -> Path | None:
    """Return the first manifest found while walking up from a starting path."""
    for parent in [start, *start.parents]:
        candidate = parent / _MANIFEST_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _find_manifest() -> Path:
    """Find the manifest file.

    Search order:
    1. BAG3D_MANIFEST_PATH environment variable
    2. Walk up from the current working directory
    3. Docker path /opt/3dbag-pipeline/3dbag-manifest.json
    4. Walk up from this file to find repo root
    """
    env_path = os.environ.get("BAG3D_MANIFEST_PATH")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p

    cwd_manifest = _walk_manifest_parents(Path.cwd())
    if cwd_manifest is not None:
        return cwd_manifest

    if _DOCKER_MANIFEST_PATH.is_file():
        return _DOCKER_MANIFEST_PATH

    current = Path(__file__).resolve().parent
    module_manifest = _walk_manifest_parents(current)
    if module_manifest is not None:
        return module_manifest

    raise FileNotFoundError(
        f"Cannot find {_MANIFEST_FILENAME}. "
        "Set BAG3D_MANIFEST_PATH or run from the repository root."
    )


def load_manifest() -> dict:
    """Parse and return the full manifest, cached after first call."""
    global _manifest_cache
    if _manifest_cache is None:
        path = _find_manifest()
        _manifest_cache = json.loads(path.read_text(encoding="utf-8"))
    assert _manifest_cache is not None
    return _manifest_cache


def get_tool_metadata(tool_name: str) -> dict:
    """Return a single tool's entry from the manifest."""
    manifest = load_manifest()
    tools = manifest.get("tools", {})
    if tool_name not in tools:
        raise KeyError(f"Tool '{tool_name}' not found in manifest")
    return tools[tool_name]
