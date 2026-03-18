"""Check that tool versions in 3dbag-manifest.json match Dockerfile ARG defaults."""

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

MANIFEST_PATH = REPO_ROOT / "3dbag-manifest.json"
DOCKERFILE_PATH = REPO_ROOT / "docker" / "tools" / "Dockerfile"

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


def main():
    if not MANIFEST_PATH.exists():
        print(f"ERROR: Manifest not found: {MANIFEST_PATH}")
        return 1
    if not DOCKERFILE_PATH.exists():
        print(f"ERROR: Dockerfile not found: {DOCKERFILE_PATH}")
        return 1

    manifest = json.loads(MANIFEST_PATH.read_text())
    tools = manifest.get("tools", {})
    dockerfile_args = parse_dockerfile_args(DOCKERFILE_PATH)

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
        print("Manifest and Dockerfile versions are in sync.")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
