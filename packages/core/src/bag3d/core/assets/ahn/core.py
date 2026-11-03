from pathlib import Path
from typing import Any

import requests
from dagster import StaticPartitionsDefinition, get_dagster_logger

from bag3d.core.assets.ahn import AHN6_TILE_IDS, AHN_TILE_IDS

logger = get_dagster_logger("ahn")

partition_definition_ahn = StaticPartitionsDefinition(sorted(AHN_TILE_IDS))


BATCH_KM = 10

_batch_ids = sorted(
    {
        f"{(int(t.split('_')[0]) // (BATCH_KM * 1000)) * (BATCH_KM * 1000):06d}_"
        f"{(int(t.split('_')[1]) // (BATCH_KM * 1000)) * (BATCH_KM * 1000):06d}"
        for t in AHN6_TILE_IDS
    }
)
partition_definition_ahn6_batches = StaticPartitionsDefinition(_batch_ids)

# AHN LAZ index files containing file URL, sha256 checksum and tile geometry.
URL_LAZ_SHA = {
    6: "https://fsn1.your-objectstorage.com/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN6_KM_PC_COPC.json",
    5: "https://fsn1.your-objectstorage.com/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN5_PC.json",
    4: "https://fsn1.your-objectstorage.com/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN4_PC.json",
    3: "https://fsn1.your-objectstorage.com/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN3_PC.json",
}


def _tile_batch(tile_id: str) -> str:
    """Compute the 10×10 km batch id for a 1×1 km AHN6 tile id.

    Tile ids are *not* zero-padded in the AHN6 index (e.g. ``58000_431000``
    vs ``150000_431000``), so we cannot reconstruct them by formatting a
    coordinate range. Instead, derive the batch from each tile id and filter.
    """
    x, y = tile_id.split("_")
    bx = (int(x) // (BATCH_KM * 1000)) * (BATCH_KM * 1000)
    by = (int(y) // (BATCH_KM * 1000)) * (BATCH_KM * 1000)
    return f"{bx:06d}_{by:06d}"


def tiles_in_batch(batch_id: str) -> list[str]:
    """Return all 1×1 km tile IDs within a 10×10 km batch block."""
    return [t for t in AHN6_TILE_IDS if _tile_batch(t) == batch_id]


def format_laz_log(fpath: Path, msg: str) -> str:
    """Formats a message as <file path>.....<msg>"""
    return f"{fpath.stem}{'.' * 5}{msg}"


def validate_new_ahn_tile_ids(tile_ids: set[str]) -> None:
    diff = tile_ids ^ AHN_TILE_IDS
    if len(diff) > 0:
        logger.warning(
            "Received AHN tile list has diverged from the one used, list must be updated. "
            f"New in index: {tile_ids - AHN_TILE_IDS}, "
            f"Removed: {AHN_TILE_IDS - tile_ids}"
        )


def validate_new_ahn6_tile_ids(index: dict) -> None:
    """Validate tile IDs from the AHN6 index against AHN6_TILE_IDS.

    Logs a warning if the index has diverged from the expected tile list.
    """
    feature_set = set(index.keys())
    diff = feature_set ^ AHN6_TILE_IDS
    if len(diff) > 0:
        logger.warning(
            "AHN6 tile list has diverged from AHN6_TILE_IDS. "
            f"New in index: {feature_set - AHN6_TILE_IDS}, "
            f"Removed: {AHN6_TILE_IDS - feature_set}"
        )


def _pc_tile_id(filename: str) -> str:
    """Derive the RD tile id from an AHN3/4/5 file name.

    ``C_01CZ1.LAZ`` -> ``01cz1`` and ``2023_C_01CZ1.LAZ`` -> ``01cz1``.
    """
    name = filename.removesuffix(".LAZ")
    if "_C_" in name:
        return name.split("_C_")[-1].lower()
    if name.startswith("C_"):
        return name[2:].lower()
    return name.lower()


def download_ahn_pc_index(
    version: int,
    with_geom: bool = True,
) -> dict[str, dict[str, Any] | None] | None:
    """Download an AHN3/4/5 tile index from the fsn1 ``PC.json``.

    Each feature provides the LAZ download URL, the SHA256 checksum and the tile
    geometry, so a single download replaces the old PDOK tile index and the
    separate checksum assets. The fsn1 geometry is already ordered
    ``[easting, northing]`` (unlike the PDOK WFS response), so no inversion is
    applied.
    """
    url = URL_LAZ_SHA[version]
    logger.info(f"Downloading AHN{version} tile index from {url}")
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Failed to download AHN{version} index: {exc}")
        return {}

    features: dict[str, dict[str, Any] | None] = {}
    for f in data.get("features", []):
        props = f.get("properties", {})
        file_url = props.get("file")
        if not file_url:
            continue
        tile_id = _pc_tile_id(file_url.split("/")[-1])
        features[tile_id] = {
            "url": file_url,
            "sha256": props.get("sha256"),
            "geometry": f.get("geometry") if with_geom else None,
        }

    validate_new_ahn_tile_ids(set(features))
    logger.info(f"AHN{version} index: {len(features)} tiles")
    return features


def download_ahn6_index(
    with_geom: bool = False,
) -> dict[str, dict[str, Any] | None]:
    """Download the AHN6 KM COPC tile index with checksums.

    Fetches the GeoJSON from URL_LAZ_SHA[6] and extracts each feature's
    file URL and SHA256 checksum. Returns a dict keyed by tile ID.
    """
    logger.info(f"Downloading AHN6 tile index from {URL_LAZ_SHA[6]}")
    try:
        resp = requests.get(URL_LAZ_SHA[6], timeout=120)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Failed to download AHN6 index: {exc}")
        return {}

    features = {}
    for f in data.get("features", []):
        props = f.get("properties", {})
        file_url = props.get("file", "")
        if not file_url:
            logger.warning(f"Skipping feature with missing file URL: {file_url}")
            continue
        filename = file_url.split("/")[-1]
        name = filename.replace(".COPC.LAZ", "").replace(".LAZ", "")
        parts = name.split("_C_")
        if len(parts) != 2:
            logger.warning(
                f"Skipping feature with unrecognized file name format: {file_url}"
            )
            continue

        tile_id = parts[1]
        features[tile_id] = {
            "url": file_url,
            "sha256": props.get("sha256"),
            "geometry": f.get("geometry") if with_geom else None,
        }

    validate_new_ahn6_tile_ids(features)
    logger.info(f"AHN6 index: {len(features)} tiles with checksums")
    return features
