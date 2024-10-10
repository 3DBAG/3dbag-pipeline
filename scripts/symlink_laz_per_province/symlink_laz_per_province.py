"""Create directories per Dutch province and create symlinks to the LAZ files that
are within each province.
The province boundaries and AHN tile index is retrieved from webservices.
"""

import logging
from argparse import ArgumentParser
from json import loads as json_loads
from pathlib import Path
from sys import stdout

from bag3d.common.utils.requests import download_as_str
from shapely import STRtree
from shapely.geometry import shape


def ahn_filename(tile_name: str) -> str:
    """Creates an AHN LAZ file name from an AHN tile name."""
    return f"C_{tile_name.upper()}.LAZ"


def configure_logging(verbosity):
    """Configures the general logging in the application"""
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(
        stream=stdout,
        level=log_level,
        format="%(asctime)s\t%(name)-24s\t%(lineno)s\t[%(levelname)-8s]\t%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


configure_logging(1)
log = logging.getLogger("symlink_laz_per_province")

parser = ArgumentParser(
    prog="symlink_laz_per_province",
    description="Create directories per Dutch province and creates symlinks to the LAZ files that are within the given province.",
)
parser.add_argument("--laz", help="Directory containing the LAZ files.")
parser.add_argument(
    "--output",
    help="Directory to store the symlinks that point to the files in '--laz'.",
)

if __name__ == "__main__":
    args = parser.parse_args()
    path_laz = Path(args.laz)
    if not path_laz.is_dir():
        raise NotADirectoryError(f"LAZ directory does not exist: {path_laz}")
    path_output = Path(args.output)

    # Provinces
    # Source provinces: https://www.nationaalgeoregister.nl/geonetwork/srv/api/records/208bc283-7c66-4ce7-8ad3-1cf3e8933fb5?language=all
    url_provinces_wfs = "https://service.pdok.nl/kadaster/bestuurlijkegebieden/wfs/v1_0"
    # !!! The space in "application/json; subtype=geojson" is required!
    params_provinces_wfs = {
        "request": "GetFeature",
        "service": "WFS",
        "version": "1.1.0",
        "outputFormat": "application/json; subtype=geojson",
        "typeName": "bestuurlijkegebieden:Provinciegebied",
    }
    log.info(f"Downloading the provinces boundaries from {url_provinces_wfs}")
    str_provinces = download_as_str(
        url=url_provinces_wfs, parameters=params_provinces_wfs
    )
    if not str_provinces.startswith("{"):
        raise ValueError(
            "Did not receive a JSON value, which probably means invalid reponse from the WFS:\n{geojson_str_provinces}"
        )
    dict_provinces = json_loads(str_provinces)

    # AHN Bladwijzer
    url_ahn_bladwijzer_wfs = (
        "https://api.ellipsis-drive.com/v3/ogc/wfs/a9d410ad-a2f6-404c-948a-fdf6b43e77a6"
    )
    params_ahn_bladwijzer_wfs = {
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
    log.info(f"Downloading the AHN tile boundaries from {url_ahn_bladwijzer_wfs}")
    str_ahn_bladwijzer = download_as_str(
        url=url_ahn_bladwijzer_wfs, parameters=params_ahn_bladwijzer_wfs
    )
    if not str_ahn_bladwijzer.startswith("{"):
        raise ValueError(
            "Did not receive a JSON value, which probably means invalid reponse from the WFS:\n{geojson_str_provinces}"
        )
    dict_ahn_bladwijzer = json_loads(str_ahn_bladwijzer)
    strtree_ahn = STRtree(
        [
            shape(feature["geometry"]).centroid
            for feature in dict_ahn_bladwijzer["features"]
        ]
    )

    # Query and symlink
    log.info(
        f"Creating symlinks for provinces in {args.output} to the LAZ files in {args.laz}"
    )
    for province in dict_provinces["features"]:
        province_name = province["properties"]["naam"]
        province_geometry = shape(province["geometry"])
        result = strtree_ahn.query(geometry=province_geometry, predicate="intersects")
        ahn_tile_names = [
            dict_ahn_bladwijzer["features"][tile_i]["properties"]["AHN"]
            for tile_i in result
        ]
        province_path = path_output / province_name.lower()
        province_path.mkdir(parents=True, exist_ok=True)
        for ahn_tile_name in ahn_tile_names:
            filename = ahn_filename(ahn_tile_name)
            (province_path / filename).symlink_to(path_laz / filename)

    log.info("Done")
