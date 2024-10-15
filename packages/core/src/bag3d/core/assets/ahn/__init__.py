from typing import Dict, Optional, Tuple

import requests


def download_ahn_index(
    with_geom: bool = False,
) -> Optional[Dict[str, Optional[Dict[str, Optional[str]]]]]:
    """Downloads the AHN 3/4/5 tile index.
    Args:
        ahn_version: The AHN version, either 3 or 4 or 5.
        with_geom: If False, request only the AHN tile ids. Else also request the
            tile boundaries as geojson.
    Returns:
        A dict of {tile id: link to laz}. If not ``with_geom``, then value of the link is None.
    """

    service_url = (
        "https://api.ellipsis-drive.com/v3/ogc/wfs/a9d410ad-a2f6-404c-948a-fdf6b43e77a6"
    )
    print(f"Downloading the AHN tile boundaries from {service_url}")
    params_features = {
        "request": "GetFeature",
        "service": "WFS",
        "preferCoordinatesForWfsT11": "true",
        "srsname": "EPSG:28992",
        "version": "1.1.0",
        "requestedEpsg": "28992",
        "outputFormat": "application/json",
        "CountDefault": "2000",
        "typeName": "layerId_14b12666-cfbb-4362-905a-8832afe5ffa8",
    }

    features = {}
    # TODO: include the tile geometry, and maybe use the download links from here

    response = requests.get(url=service_url + "/query", params=params_features)
    if response.status_code == 200:
        r_json = response.json()
    else:  # pragma: no cover
        response.raise_for_status()
        return
    returned_features = r_json.get("features")
    if returned_features is None or len(returned_features) == 0:
        return features
    else:
        if with_geom:
            for f in r_json["features"]:
                features[f["properties"]["AHN"].lower()] = {
                    "AHN3_LAZ": f["properties"]["AHN3 puntenwolk"],
                    "AHN4_LAZ": f["properties"]["AHN4 puntenwolk"],
                    "AHN5_LAZ": f["properties"]["AHN5 puntenwolk"],
                    "geometry": f["geometry"],
                }
        else:
            for f in r_json["features"]:
                features[f["properties"]["AHN"].lower()] = None

    return features


def tile_index_origin() -> Tuple[float, float, float, float]:  # pragma: no cover
    """Computes the BBOX of the AHN tile index."""
    tindex = download_ahn_index(True)
    minx, miny = tindex["01cz1"]["geometry"]["coordinates"][0][0]
    maxx, maxy = minx, miny
    for feature in tindex.values():
        exterior = feature["geometry"]["coordinates"][0]
        for x, y in exterior:
            minx = x if x < minx else minx
            miny = y if y < miny else miny
            maxx = x if x > maxx else maxx
            maxy = y if y > maxy else maxy
    return minx, miny, maxx, maxy


TILES = download_ahn_index(with_geom=True)
