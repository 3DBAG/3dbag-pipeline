from pathlib import Path

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

AHN6_INDEX_URL = (
    "https://basisdata.nl/hwh-portal/20230609_tmp/links/nationaal/Nederland/"
    "AHN6_KM_PC_COPC.json"
)


def tiles_in_batch(batch_id: str) -> list[str]:
    """Return all 1×1 km tile IDs within a 10×10 km batch block."""
    bx = int(batch_id.split("_")[0])
    by = int(batch_id.split("_")[1])
    return [
        f"{x:06d}_{y:06d}"
        for x in range(bx, bx + BATCH_KM * 1000, 1000)
        for y in range(by, by + BATCH_KM * 1000, 1000)
        if f"{x:06d}_{y:06d}" in AHN6_TILE_IDS
    ]


def format_laz_log(fpath: Path, msg: str) -> str:
    """Formats a message as <file path>.....<msg>"""
    return f"{fpath.stem}{'.' * 5}{msg}"


def validate_new_ahn_tile_ids(features: dict) -> None:
    feature_set = {f["properties"]["AHN"].lower() for f in features}
    if len(feature_set ^ AHN_TILE_IDS) > 0:
        logger.warning(
            "Received AHN tile list has diverged from the one used, list must be updated"
            f"Difference: {feature_set ^ AHN_TILE_IDS}"
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


def invert_geometry_coordinates(geometry):
    """Invert x and y coordinates in the JSON geometry."""
    if geometry["type"] == "Polygon":
        inverted_coords = []
        for ring in geometry["coordinates"]:
            inverted_ring = [[coord[1], coord[0]] for coord in ring]
            inverted_coords.append(inverted_ring)
        return {"type": "Polygon", "coordinates": inverted_coords}
    elif geometry["type"] == "MultiPolygon":
        inverted_coords = []
        for polygon in geometry["coordinates"]:
            inverted_polygon = []
            for ring in polygon:
                inverted_ring = [[coord[1], coord[0]] for coord in ring]
                inverted_polygon.append(inverted_ring)
            inverted_coords.append(inverted_polygon)
        return {"type": "MultiPolygon", "coordinates": inverted_coords}
    elif geometry["type"] == "Point":
        return {
            "type": "Point",
            "coordinates": [geometry["coordinates"][1], geometry["coordinates"][0]],
        }
    else:
        # Return original geometry for unsupported types
        return geometry


def download_ahn_index(
    with_geom: bool = False,
) -> dict[str, dict[str, str | None] | None] | None:
    """Downloads the AHN 3/4/5 tile index.
    Args:
        with_geom: If False, request only the AHN tile ids. Else also request the
            tile boundaries as geojson.
    Returns:
        A dict of {tile id: dict of links and geometry}. If not ``with_geom``, then value of the links is None.
    """

    service_url = (
        "https://api.ellipsis-drive.com/v3/ogc/wfs/a9d410ad-a2f6-404c-948a-fdf6b43e77a6"
    )
    params_features = {
        "request": "GetFeature",
        "service": "WFS",
        "preferCoordinatesForWfsT11": "true",
        "srsname": "EPSG:28992",
        "version": "1.1.0",
        "requestedEpsg": "28992",
        "outputFormat": "application/json",
        "CountDefault": "2000",
        "typeName": "layerId_05931403-2510-43af-9cc3-f60a066d4482",
    }
    logger.info(f"Downloading the AHN tile boundaries from {service_url}")

    features = {}

    response = requests.get(url=service_url + "/query", params=params_features)
    if response.status_code == 200:
        r_json = response.json()
    else:  # pragma: no cover
        response.raise_for_status()
        return
    returned_features = r_json.get("features")
    validate_new_ahn_tile_ids(returned_features)
    if returned_features is None or len(returned_features) == 0:
        logger.error(
            "The response did not contain a 'features' member or had 0 features."
        )
        return features
    else:
        if with_geom:
            for f in returned_features:
                features[f["properties"]["AHN"].lower()] = {
                    "AHN3_LAZ": f["properties"]["AHN3 puntenwolk"],
                    "AHN4_LAZ": f["properties"]["AHN4 puntenwolk"],
                    "AHN5_LAZ": f["properties"]["AHN5 puntenwolk"],
                    "geometry": invert_geometry_coordinates(f["geometry"]),
                }
        else:
            for f in returned_features:
                features[f["properties"]["AHN"].lower()] = {
                    "AHN3_LAZ": f["properties"]["AHN3 puntenwolk"],
                    "AHN4_LAZ": f["properties"]["AHN4 puntenwolk"],
                    "AHN5_LAZ": f["properties"]["AHN5 puntenwolk"],
                    "geometry": None,
                }

    return features


def download_ahn6_index(
    with_geom: bool = False,
) -> dict[str, dict[str, str | None] | None]:
    """Download the AHN6 KM COPC tile index with checksums.

    Fetches the GeoJSON from AHN6_INDEX_URL and extracts each feature's
    file URL and SHA256 checksum. Returns a dict keyed by tile ID.
    """
    logger.info(f"Downloading AHN6 tile index from {AHN6_INDEX_URL}")
    try:
        resp = requests.get(AHN6_INDEX_URL, timeout=120)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
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
            "geometry": f.get("geometry") if with_geom else None,
        }

    validate_new_ahn6_tile_ids(features)
    logger.info(f"AHN6 index: {len(features)} tiles with checksums")
    return features
