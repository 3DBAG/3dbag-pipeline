from pathlib import Path
from typing import Dict, Optional

import requests
from dagster import StaticPartitionsDefinition, get_dagster_logger
from bag3d.core import AHN_TILE_IDS, KM_TILE_IDS

logger = get_dagster_logger("ahn")

partition_definition_ahn = StaticPartitionsDefinition(sorted(list(AHN_TILE_IDS)))
partition_definition_km = StaticPartitionsDefinition(sorted(list(KM_TILE_IDS)))


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
    """Downloads the AHN 3/4/5/6 tile index.
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
                features[f["properties"]["AHN"].lower()] = None

    return features
