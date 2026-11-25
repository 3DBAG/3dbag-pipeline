"""Portable raw-AHN snapshot contract shared by core and terrain."""

import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path

DEFAULT_RADIUS_M = 500
SELECTION_MARGIN_M = 300


def padded_bounds(bounds, radius_m):
    """Include interpolation neighbors and the source-transition warp buffer."""
    west, south, east, north = bounds
    halo = radius_m + SELECTION_MARGIN_M
    return west - halo, south - halo, east + halo, north + halo


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def snapshot_path(root: Path, relative: str) -> Path:
    if not isinstance(relative, str) or Path(relative).is_absolute():
        raise ValueError(f"Expected a snapshot-relative terrain path: {relative!r}")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Terrain path escapes snapshot root: {relative!r}")
    return path


def validate_bounds(bounds):
    if (
        not isinstance(bounds, list)
        or len(bounds) != 4
        or not all(isinstance(v, (int, float)) and math.isfinite(v) for v in bounds)
        or not (bounds[0] < bounds[2] and bounds[1] < bounds[3])
    ):
        raise ValueError(f"Invalid terrain bounds: {bounds!r}")
    return bounds


def contains(outer, inner) -> bool:
    return (
        outer[0] <= inner[0]
        and outer[1] <= inner[1]
        and outer[2] >= inner[2]
        and outer[3] >= inner[3]
    )


def covers_bounds(extents, bounds) -> bool:
    """Check rectangle union coverage by merging vertical intervals in each x strip."""
    west, south, east, north = bounds
    edges = sorted(
        {west, east}
        | {x for extent in extents for x in (extent[0], extent[2]) if west < x < east}
    )
    for left, right in zip(edges, edges[1:]):
        intervals = sorted(
            (bottom, top)
            for x0, bottom, x1, top in extents
            if x0 <= left and x1 >= right
        )
        covered = south
        for bottom, top in intervals:
            if bottom > covered:
                break
            covered = max(covered, top)
        if covered < north:
            return False
    return True


def validate_terrain_catalog(catalog: dict, aoi: dict | None = None) -> None:
    """Apply the same metadata and coverage contract before publishing and reading."""
    if (
        catalog.get("version") != 1
        or catalog.get("height_reference") != "NAP"
        or catalog.get("crs") != "EPSG:28992"
        or catalog.get("resolution_m") != 5
    ):
        raise ValueError("Expected a version-1 raw 5 m RD/NAP terrain catalog")
    validate_bounds(catalog["bounds"])
    bounds_rd = validate_bounds(catalog["bounds_rd"])
    validate_bounds(catalog["selection_bounds_rd"])
    radius = catalog["selection_radius_m"]
    if not isinstance(radius, (int, float)) or not math.isfinite(radius) or radius <= 0:
        raise ValueError("Invalid terrain snapshot interpolation radius")
    if not contains(catalog["selection_bounds_rd"], padded_bounds(bounds_rd, radius)):
        raise ValueError("Terrain snapshot has insufficient interpolation neighbors")
    if aoi is not None:
        required = [
            aoi["minx"] - 10,
            aoi["miny"] - 10,
            aoi["maxx"] + 10,
            aoi["maxy"] + 10,
        ]
        if not contains(bounds_rd, validate_bounds(required)):
            raise ValueError("Terrain snapshot does not cover the integration AOI")
    records = catalog.get("files")
    if not isinstance(records, list) or not records:
        raise ValueError("Terrain snapshot contains no raw AHN rasters")
    locations = [record["path"] for record in records]
    if len(set(locations)) != len(locations):
        raise ValueError("Duplicate terrain fixture paths")
    extents = [validate_bounds(record["bounds_rd"]) for record in records]
    if not covers_bounds(extents, bounds_rd):
        raise ValueError(
            "Terrain rasters have insufficient coverage of the snapshot AOI"
        )


def terrain_snapshot(root: Path, manifest: Mapping | None = None) -> dict:
    """Verify the declared terrain catalog and files without modifying the snapshot."""
    if manifest is None:
        manifest = json.loads((root / "manifest.json").read_text())
    if not isinstance(manifest, Mapping):
        raise ValueError("Integration snapshot manifest must be a JSON object")
    if str(manifest.get("fixture_version")) not in {"2", "3"}:
        raise ValueError("Unsupported terrain integration snapshot version")
    source = manifest.get("sources", {}).get("terrain")
    if not isinstance(source, dict) or not isinstance(source.get("path"), str):
        raise ValueError(
            "Integration snapshot has no terrain data; regenerate it with integration_data"
        )
    inventory = manifest["files"]

    def verify(relative):
        path = snapshot_path(root, relative)
        record = inventory.get(relative)
        if (
            not isinstance(record, dict)
            or not path.is_file()
            or path.stat().st_size != record.get("size")
            or sha256_file(path) != record.get("sha256")
        ):
            raise ValueError(
                f"Missing, changed or undeclared terrain fixture: {relative}"
            )
        return path

    catalog = json.loads(verify(source["path"]).read_text())
    validate_terrain_catalog(catalog, manifest.get("aoi"))
    for record in catalog["files"]:
        relative = record["path"]
        verify(relative)
        declared = inventory[relative]
        if (
            record["bytes"] != declared["size"]
            or record["sha256"] != declared["sha256"]
        ):
            raise ValueError(
                f"Terrain catalog and snapshot inventory disagree: {relative}"
            )
    return catalog
