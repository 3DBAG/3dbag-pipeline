"""Set the pipeline version across 3dbag-manifest.json and all pyproject.toml files."""

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

MANIFEST_PATH = REPO_ROOT / "3dbag-manifest.json"

PYPROJECT_FILES = [
    REPO_ROOT / "pyproject.toml",
    REPO_ROOT / "packages" / "common" / "pyproject.toml",
    REPO_ROOT / "packages" / "core" / "pyproject.toml",
    REPO_ROOT / "packages" / "export" / "pyproject.toml",
    REPO_ROOT / "packages" / "floors_estimation" / "pyproject.toml",
    REPO_ROOT / "packages" / "party_walls" / "pyproject.toml",
]

VERSION_PATTERN = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")


def update_manifest(version):
    text = MANIFEST_PATH.read_text()
    text = re.sub(
        r'^(\s*"version"\s*:\s*")[^"]+(")',
        rf"\g<1>{version}\2",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    MANIFEST_PATH.write_text(text)


def update_pyproject(path, version):
    text = path.read_text()
    # Update version = "..." in [project] section
    text = re.sub(
        r'^(version\s*=\s*")[^"]+(")',
        rf"\g<1>{version}\2",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    # Update current_version = "..." in [tool.bumpver] section (root pyproject.toml)
    text = re.sub(
        r'^(current_version\s*=\s*")[^"]+(")',
        rf"\g<1>{version}\2",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    path.write_text(text)


def main():
    parser = argparse.ArgumentParser(description="Set pipeline version everywhere.")
    parser.add_argument("version", help="Version string (YYYY.MM.DD)")
    args = parser.parse_args()

    if not VERSION_PATTERN.match(args.version):
        print(f"ERROR: Invalid version format '{args.version}', expected YYYY.MM.DD")
        return 1

    update_manifest(args.version)
    print(f"Updated {MANIFEST_PATH.name}")

    for path in PYPROJECT_FILES:
        if not path.exists():
            print(f"WARNING: {path} not found, skipping")
            continue
        update_pyproject(path, args.version)
        print(f"Updated {path.relative_to(REPO_ROOT)}")

    print(f"\nVersion set to {args.version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
