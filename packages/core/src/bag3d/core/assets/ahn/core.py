from pathlib import Path
from typing import Dict, Optional

import requests
from dagster import StaticPartitionsDefinition, get_dagster_logger
from bag3d.core.assets.ahn import AHN_TILE_IDS, AHN6_TILE_IDS

logger = get_dagster_logger("ahn")

partition_definition_ahn = StaticPartitionsDefinition(sorted(list(AHN_TILE_IDS)))


BATCH_KM = 10

_batch_ids = sorted(
    {
        f"{(int(t.split('_')[0]) // (BATCH_KM * 1000)) * (BATCH_KM * 1000):06d}_"
        f"{(int(t.split('_')[1]) // (BATCH_KM * 1000)) * (BATCH_KM * 1000):06d}"
        for t in AHN6_TILE_IDS
    }
)
partition_definition_km_batches = StaticPartitionsDefinition(_batch_ids)

AHN6_API_URL = (
    "https://api.ellipsis-drive.com/v3/ogc/features/"
    "0820faae-5240-499b-8486-cf406433cf71/collections/"
    "6aec07f5-f7eb-4f51-b6f7-aee45e5767bd/items"
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


def validate_ahn6_tile_ids() -> None:
    """Fetch live AHN6 tile IDs from the OGC Features API and compare against AHN6_TILE_IDS.

    Logs a warning if the tile list has diverged (new tiles added or removed).
    """
    try:
        tile_ids = set()
        url = f"{AHN6_API_URL}?limit=2000"
        while url:
            resp = requests.get(url, timeout=60)
            data = resp.json()
            for f in data["features"]:
                name = f["properties"]["AHN"]
                laz = f["properties"].get("Puntenwolk")
                if laz:
                    tile_ids.add(name)
            returned = data.get("numberReturned", 0)
            if returned < 2000:
                break
            url = None
            for link in data.get("links", []):
                if link.get("rel") == "next":
                    url = link["href"]
                    break
    except Exception as exc:
        logger.warning(f"Failed to validate AHN6 tile IDs: {exc}")
        return

    if len(tile_ids ^ AHN6_TILE_IDS) > 0:
        logger.warning(
            "AHN6 tile list has diverged from the one in __init__.py. "
            f"New: {tile_ids - AHN6_TILE_IDS}, "
            f"Removed: {AHN6_TILE_IDS - tile_ids}"
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
) -> Optional[Dict[str, Optional[Dict[str, Optional[str]]]]]:
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


def ahn6_tile_geometry(tile_id: str) -> dict:
    """Return a GeoJSON Polygon for a 1×1 km AHN6 tile.

    Tile IDs are ``"XXXXXX_YYYYYY"`` (RD coordinates in metres).
    The polygon covers the 1×1 km cell.
    """
    x_min = int(tile_id.split("_")[0])
    y_min = int(tile_id.split("_")[1])
    x_max = x_min + 1000
    y_max = y_min + 1000
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [x_min, y_min],
                [x_max, y_min],
                [x_max, y_max],
                [x_min, y_max],
                [x_min, y_min],
            ]
        ],
    }
