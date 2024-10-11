from pathlib import Path
from typing import Union, Tuple, Dict, Optional
from math import ceil

import requests
from dagster import StaticPartitionsDefinition


class PartitionDefinitionAHN(StaticPartitionsDefinition):
    def __init__(self, ahn_version: int):
        tile_ids = download_ahn_index(with_geom=False)
        super().__init__(partition_keys=sorted(list(tile_ids)))


def format_laz_log(fpath: Path, msg: str) -> str:
    """Formats a message as <file path>.....<msg>"""
    return f"{fpath.stem}{'.' * 5}{msg}"


def ahn_filename(tile_id: str) -> str:
    """Creates an AHN LAZ file name from an AHN tile ID."""
    return f"C_{tile_id.upper()}.LAZ"


def ahn_dir(root_dir: Path, ahn_version: int) -> Path:
    """Create a directory path where to store the AHN LAZ files for the given AHN
    version."""
    return Path(root_dir) / "pointcloud" / f"AHN{ahn_version}"


def ahn_laz_dir(root_dir: Path, ahn_version: int) -> Path:
    """Create a directory path where to store the AHN LAZ files for the given AHN
    version."""
    return ahn_dir(root_dir, ahn_version) / "as_downloaded" / "LAZ"


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
    print(f"Downloading the AHN tile boundaries from {service_url}")

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
    # 13000 306250 279000 616250
    return minx, miny, maxx, maxy


def generate_grid(bbox: Tuple[float, float, float, float], cellsize: int):
    """Generates a grid of fixed cell-size for a BBOX.
    The origin of the grid is the BBOX min coordinates.

    Args:
        bbox: (minx, miny, maxx, maxy)
        cellsize: Cell size.

    Returns:
        The bbox of the generated grid, nr. of cells in X-direction,
        nr. of cells in Y-direction.
    """
    origin = bbox[:2]
    nr_cells_x = ceil((bbox[2] - bbox[0]) / cellsize)
    nr_cells_y = ceil((bbox[3] - bbox[1]) / cellsize)
    bbox_new = (
        *origin,
        origin[0] + nr_cells_x * cellsize,
        origin[1] + nr_cells_y * cellsize,
    )
    return bbox_new, nr_cells_x, nr_cells_y
