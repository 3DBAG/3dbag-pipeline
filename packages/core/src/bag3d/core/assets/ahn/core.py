from pathlib import Path
from typing import Union, Tuple
from math import ceil

import requests
from dagster import StaticPartitionsDefinition


class PartitionDefinitionAHN(StaticPartitionsDefinition):
    def __init__(self, ahn_version: int):
        tile_ids = download_ahn_index_esri(ahn_version, with_geom=False)
        super().__init__(partition_keys=sorted(list(tile_ids)))


def format_laz_log(fpath: Path, msg: str) -> str:
    """Formats a message as <file path>.....<msg>"""
    return f"{fpath.stem}{'.' * 5}{msg}"


def ahn_filename(tile_id: str) -> str:
    """Creates an AHN LAZ file name from an AHN tile ID."""
    return f'C_{tile_id.upper()}.LAZ'


def ahn_dir(root_dir: Path, ahn_version: int) -> Path:
    """Create a directory path where to store the AHN LAZ files for the given AHN
    version."""
    return Path(root_dir) / "pointcloud" / f"AHN{ahn_version}"


def ahn_laz_dir(root_dir: Path, ahn_version: int) -> Path:
    """Create a directory path where to store the AHN LAZ files for the given AHN
    version."""
    return ahn_dir(root_dir, ahn_version) / "as_downloaded" / "LAZ"


def download_ahn_index_esri(ahn_version: int, with_geom: bool = False) -> Union[
    dict, None]:
    """Download the AHN3/4 tile index from the esri layer that is available in the
    official https://www.ahn.nl/ahn-viewer.

    If a feature is available in the 'Kaartbladen_AHN4/FeatureServer', then that
    LAZ file is available for download.

    Working with an esri FeatureServer: https://gis.stackexchange.com/a/427446

    Examples:

        AHN3 feature:
        ```
        {
          "type": "Feature",
          "id": 1663,
          "geometry": {"type": "Polygon", "coordinates": [[[45000, 387500], [45000, 393750], [50000, 393750], [50000, 387500], [45000, 387500]]]},
          "properties": {
            "OBJECTID": 1663,
            "CenterX": 47500,
            "CenterY": 390625,
            "Shape_Leng": 22500,
            "ahn2_05m_i": "http://geodata.nationaalgeoregister.nl/ahn2/extract/ahn2_05m_int/i65gn2.tif.zip",
            "ahn2_05m_n": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/DTM_50cm/i65gn2.tif.zip",
            "ahn2_05m_r": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/DSM_50cm/r65gn2.tif.zip",
            "ahn2_5m": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/DTM_5m/ahn2_5_65gn2.tif.zip",
            "ahn2_LAZ_g": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/ahn2_laz_units_gefilterd/g65gn2.laz",
            "ahn2_LAZ_u": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN2/ahn2_laz_units_uitgefilterd/u65gn2.laz",
            "Kaartblad": "65gn2",
            "ahn1_5m": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN1/DTM_5m/65gn2.tif.zip",
            "ahn1_LAZ_g": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN1/ahn1_gefilterd/65gn2.laz.zip",
            "ahn1_LAZ_u": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN1/ahn1_uitgefilterd/u65gn2.laz.zip",
            "AHN3_05m_DSM": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/DSM_50cm/R_65GN2.zip",
            "AHN3_05m_DTM": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/DTM_50cm/M_65GN2.zip",
            "AHN3_5m_DSM": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/DSM_5m/R5_65GN2.zip",
            "AHN3_5m_DTM": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/DTM_5m/M5_65GN2.zip",
            "AHN3_LAZ": "https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/LAZ/C_65GN2.LAZ",
            "AHN_Beschikbaar": "AHN1, AHN2, AHN3"
          }
        }
        ```

        AHN4 feature:
        ```
        {
          "type": "Feature",
          "id": 1,
          "geometry": {"type": "Polygon", "coordinates": [[[140000, 600000], [140000, 606250], [145000, 606250], [145000, 600000], [140000, 600000]]]},
          "properties": {
            "OBJECTID": 1,
            "Name": "01CZ1",
            "AHN4_DTM_05m": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/02a_DTM_0.5m/M_01CZ1.zip",
            "AHN4_DTM_5m": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/02b_DTM_5m/M5_01CZ1.zip",
            "AHN4_DSM_05m": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/03a_DSM_0.5m/R_01CZ1.zip",
            "AHN4_DSM_5m": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/03b_DSM_5m/R5_01CZ1.zip",
            "AHN4_LAZ": "https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/01_LAZ/C_01CZ1.LAZ",
            "Shape__Area": 31250000, "Shape__Length": 22500
          }
        }
        ```

    Args:
        ahn_version: The AHN version, either 3 or 4.
        with_geom: If False, request only the AHN tile ids. Else also request the
            tile boundaries as geojson.
    Returns:
        A dict of {tile id: feature}. If not ``with_geom``, then feature is None.
    """
    service_url = f"https://services.arcgis.com/nSZVuSZjHpEZZbRo/arcgis/rest/services/Kaartbladen_AHN{ahn_version}/FeatureServer/0"
    # Paginate
    page_size = 500
    # TODO: include the tile geometry, and maybe use the download links from here
    tile_id_field = "Name" if ahn_version == 4 else "Kaartblad"
    outfields = tile_id_field if not with_geom else "*"
    params_features = {
        "returnIdsOnly": False,
        "f": "geojson",
        "where": "1=1",
        "outFields": outfields,
        "supportsPagination": True,
        "resultOffset": 0,
        "resultRecordCount": page_size,
        "returnGeometry": with_geom,
        "outSR": 28992
    }
    features = {}
    while True:
        response = requests.get(url=service_url + "/query", params=params_features)
        if response.status_code == 200:
            r_json = response.json()
        else: # pragma: no cover
            response.raise_for_status()
            return
        returned_features = r_json.get("features")
        if returned_features is None or len(returned_features) == 0:
            break
        else:
            if with_geom:
                for f in r_json["features"]:
                    features[f["properties"][tile_id_field].lower()] = f
            else:
                for f in r_json["features"]:
                    features[f["properties"][tile_id_field].lower()] = None
            params_features["resultOffset"] += page_size
    return features


def tile_index_origin() -> Tuple[float, float, float, float]: # pragma: no cover
    """Computes the BBOX of the AHN tile index."""
    tindex = download_ahn_index_esri(3, True)
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
    bbox_new = (*origin, origin[0] + nr_cells_x * cellsize,
                origin[1] + nr_cells_y * cellsize)
    return bbox_new, nr_cells_x, nr_cells_y
