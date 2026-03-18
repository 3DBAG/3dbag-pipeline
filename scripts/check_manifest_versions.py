"""Check that versions in 3dbag-manifest.json match Dockerfile ARGs and pyproject.toml files."""

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

MANIFEST_PATH = REPO_ROOT / "3dbag-manifest.json"
DOCKERFILE_PATH = REPO_ROOT / "docker" / "tools" / "Dockerfile"

PYPROJECT_FILES = [
    REPO_ROOT / "pyproject.toml",
    REPO_ROOT / "packages" / "common" / "pyproject.toml",
    REPO_ROOT / "packages" / "core" / "pyproject.toml",
    REPO_ROOT / "packages" / "export" / "pyproject.toml",
    REPO_ROOT / "packages" / "floors_estimation" / "pyproject.toml",
    REPO_ROOT / "packages" / "party_walls" / "pyproject.toml",
]

# Mapping from manifest tool key to Dockerfile ARG name
TOOL_TO_ARG = {
    "uv": "UV_VERSION",
    "gdal": "GDAL_VERSION",
    "proj": "PROJ_VERSION",
    "geos": "GEOS_VERSION",
    "geotiff": "GEOTIFF_VERSION",
    "lastools": "LASTOOLS_VERSION",
    "pdal": "PDAL_VERSION",
    "val3dity": "VAL3DITY_VERSION",
    "cjio": "CJIO_VERSION",
    "tyler": "TYLER_VERSION",
    "tyler-multiformat": "TYLER_MULTIFORMAT_VERSION",
    "tyler-db": "TYLER_DB_VERSION",
    "roofer": "ROOFER_VERSION",
    "geoflow-bundle": "GEOFLOW_BUNDLE_VERSION",
    "cjval": "CJVAL_VERSION",
}


def parse_dockerfile_args(dockerfile_path):
    """Extract ARG NAME=VALUE defaults from the first occurrence of each ARG."""
    args = {}
    pattern = re.compile(r"^ARG\s+(\w+)=(.+)$")
    text = dockerfile_path.read_text()
    for line in text.splitlines():
        m = pattern.match(line.strip())
        if m:
            name, value = m.group(1), m.group(2)
            # Only keep the first occurrence (the top-level default)
            if name not in args:
                args[name] = value
    return args


def parse_pyproject_version(path):
    """Extract version = "..." from a pyproject.toml file."""
    pattern = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)
    m = pattern.search(path.read_text())
    return m.group(1) if m else None


def parse_pyproject_bumpver_version(path):
    """Extract current_version = "..." from [tool.bumpver] in root pyproject.toml."""
    pattern = re.compile(r'^current_version\s*=\s*"([^"]+)"', re.MULTILINE)
    m = pattern.search(path.read_text())
    return m.group(1) if m else None


def check_tool_versions(manifest, dockerfile_args):
    """Check tool versions between manifest and Dockerfile. Returns True if all OK."""
    tools = manifest.get("tools", {})
    mismatches = []
    missing_in_manifest = []
    missing_in_dockerfile = []

    for tool_key, arg_name in TOOL_TO_ARG.items():
        manifest_version = tools.get(tool_key, {}).get("version")
        dockerfile_version = dockerfile_args.get(arg_name)

        if manifest_version is None:
            missing_in_manifest.append((tool_key, arg_name))
            continue
        if dockerfile_version is None:
            missing_in_dockerfile.append((tool_key, arg_name))
            continue
        if manifest_version != dockerfile_version:
            mismatches.append((tool_key, arg_name, manifest_version, dockerfile_version))

    ok = True

    if missing_in_manifest:
        ok = False
        print("Tools missing from manifest:")
        for tool_key, arg_name in missing_in_manifest:
            print(f"  {tool_key} (expected for Dockerfile ARG {arg_name})")

    if missing_in_dockerfile:
        ok = False
        print("Dockerfile ARGs missing:")
        for tool_key, arg_name in missing_in_dockerfile:
            print(f"  {arg_name} (expected for manifest tool {tool_key})")

    if mismatches:
        ok = False
        print("Version mismatches between manifest and Dockerfile:")
        for tool_key, arg_name, m_ver, d_ver in mismatches:
            print(f"  {tool_key}: manifest={m_ver}  Dockerfile({arg_name})={d_ver}")

    if ok:
        print("Tool versions: manifest and Dockerfile are in sync.")

    return ok


def check_pipeline_version(manifest):
    """Check pipeline version between manifest and all pyproject.toml files. Returns True if all OK."""
    manifest_version = manifest.get("version")
    if manifest_version is None:
        print("ERROR: No 'version' field in manifest")
        return False

    ok = True
    mismatches = []

    for path in PYPROJECT_FILES:
        if not path.exists():
            print(f"WARNING: {path.relative_to(REPO_ROOT)} not found")
            ok = False
            continue
        pyproject_version = parse_pyproject_version(path)
        if pyproject_version is None:
            print(f"WARNING: No version found in {path.relative_to(REPO_ROOT)}")
            ok = False
            continue
        if pyproject_version != manifest_version:
            mismatches.append((path, pyproject_version))

    # Check bumpver current_version in root pyproject.toml
    root_pyproject = REPO_ROOT / "pyproject.toml"
    bumpver_version = parse_pyproject_bumpver_version(root_pyproject)
    if bumpver_version is not None and bumpver_version != manifest_version:
        mismatches.append((root_pyproject / "[tool.bumpver]", bumpver_version))

    if mismatches:
        ok = False
        print(f"Pipeline version mismatches (manifest={manifest_version}):")
        for path, ver in mismatches:
            rel = path.relative_to(REPO_ROOT) if hasattr(path, "relative_to") else path
            print(f"  {rel}: {ver}")

    if ok:
        print(f"Pipeline version: {manifest_version} in sync across all files.")

    return ok


def main():
    if not MANIFEST_PATH.exists():
        print(f"ERROR: Manifest not found: {MANIFEST_PATH}")
        return 1
    if not DOCKERFILE_PATH.exists():
        print(f"ERROR: Dockerfile not found: {DOCKERFILE_PATH}")
        return 1

    manifest = json.loads(MANIFEST_PATH.read_text())
    dockerfile_args = parse_dockerfile_args(DOCKERFILE_PATH)

    tools_ok = check_tool_versions(manifest, dockerfile_args)
    pipeline_ok = check_pipeline_version(manifest)

    if tools_ok and pipeline_ok:
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
