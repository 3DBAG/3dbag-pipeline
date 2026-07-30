#!/usr/bin/env python3
"""Resolve and validate versions owned by 3dbag-manifest.json."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

PIPELINE_VERSION_PATTERN = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
TOOLS_IMAGE_VERSION_PATTERN = re.compile(r"^\d{4}\.\d{2}\.\d{2}(?:\.\d+)?$")
DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
PYPROJECT_FILES = (
    "pyproject.toml",
    "packages/common/pyproject.toml",
    "packages/core/pyproject.toml",
    "packages/export/pyproject.toml",
    "packages/floors_estimation/pyproject.toml",
    "packages/party_walls/pyproject.toml",
)
LOCKFILE_PACKAGES = {
    "uv.lock": ("3dbag-pipeline",),
    "packages/common/uv.lock": ("bag3d-common",),
    "packages/core/uv.lock": ("bag3d-common", "bag3d-core"),
    "packages/export/uv.lock": ("bag3d-common", "bag3d-export"),
    "packages/floors_estimation/uv.lock": ("bag3d-common", "bag3d-floors-estimation"),
    "packages/party_walls/uv.lock": ("bag3d-common", "bag3d-party-walls"),
}
DOCKER_TOOL_ARGS = {
    "uv": "UV_IMAGE",
    "tyler": "TYLER_IMAGE",
    "tyler-db": "TYLER_DB_IMAGE",
    "roofer": "ROOFER_IMAGE",
    "cjval": "CJVAL_IMAGE",
}
SOURCE_TOOL_ARGS = {
    "gdal": "GDAL_VERSION",
    "proj": "PROJ_VERSION",
    "geos": "GEOS_VERSION",
    "geotiff": "GEOTIFF_VERSION",
    "lastools": "LASTOOLS_VERSION",
    "pdal": "PDAL_VERSION",
    "val3dity": "VAL3DITY_VERSION",
    "cjio": "CJIO_VERSION",
}
PIPELINE_IMAGES = {
    "core": "BAG3D_CORE_IMAGE",
    "floors-estimation": "BAG3D_FLOORS_ESTIMATION_IMAGE",
    "party-walls": "BAG3D_PARTY_WALLS_IMAGE",
    "export": "BAG3D_EXPORT_IMAGE",
    "dagster": "BAG3D_DAGSTER_IMAGE",
}


def repo_root() -> Path:
    """Return the repository root containing this script."""
    return Path(__file__).parent.parent


def manifest_path() -> Path:
    """Return the manifest path."""
    return repo_root() / "3dbag-manifest.json"


def load_manifest() -> dict[str, Any]:
    """Load the manifest after confirming that it exists."""
    path = manifest_path()
    if not path.exists():
        raise ValueError(f"Manifest not found: {path}")
    content = path.read_text(encoding="utf-8")
    manifest = json.loads(content)
    if not isinstance(manifest, dict):
        raise ValueError("Manifest root must be a JSON object")
    return manifest


def require_mapping(value: Any, name: str) -> dict[str, Any]:
    """Return a JSON object or raise a descriptive validation error."""
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def require_string(value: Any, name: str) -> str:
    """Return a non-empty string or raise a descriptive validation error."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def validate_manifest(manifest: dict[str, Any]) -> None:
    """Validate all version and image inputs used by build tooling."""
    version = require_string(manifest.get("version"), "version")
    if PIPELINE_VERSION_PATTERN.fullmatch(version) is None:
        raise ValueError("version must use YYYY.MM.DD format")

    images = require_mapping(manifest.get("images"), "images")
    for image_name in ("tools", *PIPELINE_IMAGES):
        image = require_mapping(images.get(image_name), f"images.{image_name}")
        require_string(image.get("repository"), f"images.{image_name}.repository")
    tools_image_version = require_string(images["tools"].get("version"), "images.tools.version")
    if TOOLS_IMAGE_VERSION_PATTERN.fullmatch(tools_image_version) is None:
        raise ValueError("images.tools.version must use YYYY.MM.DD or YYYY.MM.DD.N format")
    tools_image_digest = require_string(images["tools"].get("digest"), "images.tools.digest")
    if DIGEST_PATTERN.fullmatch(tools_image_digest) is None:
        raise ValueError("images.tools.digest must be a SHA-256 digest")

    tools = require_mapping(manifest.get("tools"), "tools")
    for tool_name in (*DOCKER_TOOL_ARGS, *SOURCE_TOOL_ARGS):
        tool = require_mapping(tools.get(tool_name), f"tools.{tool_name}")
        require_string(tool.get("version"), f"tools.{tool_name}.version")
        if tool_name in DOCKER_TOOL_ARGS:
            image = require_mapping(tool.get("image"), f"tools.{tool_name}.image")
            require_string(image.get("repository"), f"tools.{tool_name}.image.repository")
            digest = require_string(image.get("digest"), f"tools.{tool_name}.image.digest")
            if DIGEST_PATTERN.fullmatch(digest) is None:
                raise ValueError(f"tools.{tool_name}.image.digest must be a SHA-256 digest")


def image_reference(repository: str, version: str, digest: str | None = None) -> str:
    """Build a readable, optionally immutable Docker image reference."""
    reference = f"{repository}:{version}"
    if digest is not None:
        return f"{reference}@{digest}"
    return reference


def uv_version(version: str) -> str:
    """Return the normalized PEP 440 representation used by uv lockfiles."""
    return ".".join(str(int(part)) for part in version.split("."))


def environment_values(manifest: dict[str, Any]) -> dict[str, str]:
    """Return Compose and Make environment values derived from the manifest."""
    images = require_mapping(manifest["images"], "images")
    version = require_string(manifest["version"], "version")
    tools_image = require_mapping(images["tools"], "images.tools")
    values = {
        "BAG3D_PIPELINE_VERSION": version,
        "BAG3D_TOOLS_IMAGE_TAG": image_reference(
            require_string(tools_image["repository"], "images.tools.repository"),
            require_string(tools_image["version"], "images.tools.version"),
        ),
        "BAG3D_TOOLS_IMAGE": image_reference(
            require_string(tools_image["repository"], "images.tools.repository"),
            require_string(tools_image["version"], "images.tools.version"),
            require_string(tools_image["digest"], "images.tools.digest"),
        ),
    }
    for image_name, variable_name in PIPELINE_IMAGES.items():
        image = require_mapping(images[image_name], f"images.{image_name}")
        values[variable_name] = image_reference(
            require_string(image["repository"], f"images.{image_name}.repository"), version
        )
    return values


def docker_build_args(manifest: dict[str, Any]) -> dict[str, str]:
    """Return Docker build arguments for the tools image."""
    tools = require_mapping(manifest["tools"], "tools")
    values: dict[str, str] = {}
    for tool_name, argument_name in DOCKER_TOOL_ARGS.items():
        tool = require_mapping(tools[tool_name], f"tools.{tool_name}")
        image = require_mapping(tool["image"], f"tools.{tool_name}.image")
        values[argument_name] = image_reference(
            require_string(image["repository"], f"tools.{tool_name}.image.repository"),
            require_string(tool["version"], f"tools.{tool_name}.version"),
            require_string(image["digest"], f"tools.{tool_name}.image.digest"),
        )
    for tool_name, argument_name in SOURCE_TOOL_ARGS.items():
        tool = require_mapping(tools[tool_name], f"tools.{tool_name}")
        values[argument_name] = require_string(tool["version"], f"tools.{tool_name}.version")
    return values


def manifest_at_revision(revision: str) -> dict[str, Any]:
    """Load the manifest committed at a Git revision."""
    try:
        result = subprocess.run(
            ["git", "show", f"{revision}:3dbag-manifest.json"],
            check=True,
            capture_output=True,
            text=True,
            cwd=repo_root(),
        )
    except subprocess.CalledProcessError as error:
        message = error.stderr.strip() or "git show failed"
        raise ValueError(f"Cannot read manifest at Git revision {revision}: {message}") from error
    manifest = json.loads(result.stdout)
    if not isinstance(manifest, dict):
        raise ValueError(f"Manifest at Git revision {revision} must be a JSON object")
    return manifest


def changed_tools_build_paths(base_revision: str) -> set[str]:
    """Return tracked tools-image build paths changed since a Git revision."""
    try:
        result = subprocess.run(
            [
                "git",
                "diff",
                "--name-only",
                base_revision,
                "HEAD",
                "--",
                "docker/tools",
                "tools-build.sh",
                "tools-test.sh",
                "scripts/manifest_versions.py",
            ],
            check=True,
            capture_output=True,
            text=True,
            cwd=repo_root(),
        )
    except subprocess.CalledProcessError as error:
        message = error.stderr.strip() or "git diff failed"
        raise ValueError(f"Cannot compare tools-image inputs with {base_revision}: {message}") from error
    return {path for path in result.stdout.splitlines() if path}


def tools_image_publish_required(manifest: dict[str, Any], base_revision: str) -> bool:
    """Return whether a change requires publishing a new tools image."""
    previous = manifest_at_revision(base_revision)
    current_tools = require_mapping(manifest["tools"], "tools")
    previous_tools = require_mapping(previous.get("tools"), "previous tools")
    current_tools_image = require_mapping(manifest["images"]["tools"], "images.tools")
    previous_images = require_mapping(previous.get("images"), "previous images")
    previous_tools_image = require_mapping(previous_images.get("tools"), "previous images.tools")

    tools_changed = current_tools != previous_tools or bool(changed_tools_build_paths(base_revision))
    publish_reference_changed = any(
        current_tools_image.get(field) != previous_tools_image.get(field)
        for field in ("repository", "version")
    )
    if tools_changed and current_tools_image["version"] == previous_tools_image.get("version"):
        raise ValueError(
            "Tools-image inputs changed but images.tools.version was not bumped"
        )
    return tools_changed or publish_reference_changed


def set_tools_image_digest(manifest: dict[str, Any], digest: str, expected_version: str) -> None:
    """Replace only the tools-image digest after CI publishes its expected tag."""
    if DIGEST_PATTERN.fullmatch(digest) is None:
        raise ValueError("digest must be a SHA-256 digest")
    tools_image = require_mapping(manifest["images"]["tools"], "images.tools")
    if tools_image["version"] != expected_version:
        raise ValueError(
            "images.tools.version changed while publishing; refusing to write a stale digest"
        )
    path = manifest_path()
    content = path.read_text(encoding="utf-8")
    updated, replacements = re.subn(
        r'("images"\s*:\s*\{\s*"tools"\s*:\s*\{.*?"digest"\s*:\s*")[^"]+(")',
        rf"\g<1>{digest}\g<2>",
        content,
        count=1,
        flags=re.DOTALL,
    )
    if replacements != 1:
        raise ValueError("Could not locate images.tools.digest in the manifest")
    path.write_text(updated, encoding="utf-8")


def sync_pyproject_versions(manifest: dict[str, Any]) -> list[Path]:
    """Synchronize package metadata versions from the manifest."""
    version = require_string(manifest["version"], "version")
    changed_paths: list[Path] = []
    for relative_path in PYPROJECT_FILES:
        path = repo_root() / relative_path
        if not path.exists():
            raise ValueError(f"Expected project metadata file not found: {relative_path}")
        content = path.read_text(encoding="utf-8")
        updated = re.sub(
            r'^(version\s*=\s*")[^"]+(")',
            rf"\g<1>{version}\2",
            content,
            count=1,
            flags=re.MULTILINE,
        )
        updated = re.sub(
            r'^(current_version\s*=\s*")[^"]+(")',
            rf"\g<1>{version}\2",
            updated,
            count=1,
            flags=re.MULTILINE,
        )
        if updated != content:
            path.write_text(updated, encoding="utf-8")
            changed_paths.append(path)
    return changed_paths


def sync_lockfile_versions(manifest: dict[str, Any]) -> list[Path]:
    """Synchronize local-package versions recorded in uv lockfiles."""
    version = uv_version(require_string(manifest["version"], "version"))
    changed_paths: list[Path] = []
    for relative_path, package_names in LOCKFILE_PACKAGES.items():
        path = repo_root() / relative_path
        if not path.exists():
            raise ValueError(f"Expected lockfile not found: {relative_path}")
        content = path.read_text(encoding="utf-8")
        updated = content
        for package_name in package_names:
            pattern = re.compile(
                rf'(\[\[package\]\]\nname = "{re.escape(package_name)}".*?\nversion = ")[^"]+(")',
                re.DOTALL,
            )
            updated, replacements = pattern.subn(rf"\g<1>{version}\2", updated, count=1)
            if replacements != 1:
                raise ValueError(
                    f"Expected one {package_name} package entry in {relative_path}, found {replacements}"
                )
        if updated != content:
            path.write_text(updated, encoding="utf-8")
            changed_paths.append(path)
    return changed_paths


def verify_synchronized_versions(manifest: dict[str, Any]) -> None:
    """Confirm that generated package metadata matches the manifest version."""
    version = require_string(manifest["version"], "version")
    stale_paths: list[str] = []
    for relative_path in PYPROJECT_FILES:
        path = repo_root() / relative_path
        if not path.exists() or version not in path.read_text(encoding="utf-8"):
            stale_paths.append(relative_path)
    lock_version = uv_version(version)
    for relative_path in LOCKFILE_PACKAGES:
        path = repo_root() / relative_path
        if not path.exists() or lock_version not in path.read_text(encoding="utf-8"):
            stale_paths.append(relative_path)
    if stale_paths:
        raise ValueError(f"Version metadata is not synchronized: {', '.join(stale_paths)}")


def emit_values(values: dict[str, str], output_format: str) -> None:
    """Write derived values in a format accepted by the selected consumer."""
    for key, value in values.items():
        if output_format == "shell":
            print(f"export {key}={shlex.quote(value)}")
        else:
            separator = "=" if output_format == "build-args" else " := "
            print(f"{key}{separator}{value}")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line interface parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate", help="validate the manifest")
    subparsers.add_parser("check", help="validate the manifest and synchronized metadata")
    subparsers.add_parser("sync", help="synchronize package versions from the manifest")
    env_parser = subparsers.add_parser("env", help="emit Make environment assignments")
    env_parser.add_argument(
        "--format", choices=("make", "build-args", "shell"), default="make"
    )
    subparsers.add_parser("build-args", help="emit Docker tools-image build arguments")
    publish_check_parser = subparsers.add_parser(
        "tools-image-publish-required",
        help="report whether changes since a Git revision require publishing a tools image",
    )
    publish_check_parser.add_argument("base_revision")
    digest_parser = subparsers.add_parser(
        "set-tools-image-digest", help="set the digest produced by a tools-image publication"
    )
    digest_parser.add_argument("digest")
    digest_parser.add_argument("--expected-version", required=True)
    value_parser = subparsers.add_parser("value", help="print a manifest-owned version")
    value_parser.add_argument("name", choices=("pipeline", "tools"))
    return parser


def run(args: argparse.Namespace) -> None:
    """Run the requested manifest operation."""
    manifest = load_manifest()
    validate_manifest(manifest)
    if args.command == "validate":
        print("Manifest versions are valid.")
    elif args.command == "check":
        verify_synchronized_versions(manifest)
        print("Manifest and generated version metadata are synchronized.")
    elif args.command == "sync":
        changed_paths = [
            *sync_pyproject_versions(manifest),
            *sync_lockfile_versions(manifest),
        ]
        if changed_paths:
            for path in changed_paths:
                print(f"Updated {path.relative_to(repo_root())}")
        else:
            print("Package versions are already synchronized.")
    elif args.command == "env":
        emit_values(environment_values(manifest), args.format)
    elif args.command == "build-args":
        emit_values(docker_build_args(manifest), "build-args")
    elif args.command == "tools-image-publish-required":
        print(str(tools_image_publish_required(manifest, args.base_revision)).lower())
    elif args.command == "set-tools-image-digest":
        set_tools_image_digest(manifest, args.digest, args.expected_version)
        print("Updated images.tools.digest.")
    elif args.command == "value":
        if args.name == "pipeline":
            print(manifest["version"])
        else:
            images = require_mapping(manifest["images"], "images")
            tools_image = require_mapping(images["tools"], "images.tools")
            print(tools_image["version"])


def main() -> None:
    """Run the CLI with a user-facing error boundary."""
    parser = build_parser()
    args = parser.parse_args()
    try:
        run(args)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
