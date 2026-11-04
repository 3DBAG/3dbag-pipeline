"""Versioned integration snapshot geometry and processing contract."""

import math

FIXTURE_VERSION = "3"
VECTOR_PROCESSING = {
    "bag": "native_subset",
    "bgt": "pdok_aoi_whole_objects",
    "top10nl": "pdok_aoi_whole_objects",
    "cbs_buurtkaart": "gdal",
}
NATIVE_GML = {
    "bgt": {
        "namespace": "http://www.geostandaarden.nl/imgeo/2.1/simple/gml31",
        "layer": "Pand",
        "geometry": "geometrie2d",
        "xsd": "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd",
    },
    "top10nl": {
        "namespace": "http://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0",
        "layer": "Gebouw",
        "geometry": "geometrie",
        "xsd": "https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd",
    },
}


def validate_native_extent(
    extent: tuple[float, ...], aoi: tuple[float, ...], label: str
) -> None:
    """Whole source objects may cross the AOI boundary."""
    if (
        len(extent) != 4
        or not all(math.isfinite(value) for value in extent)
        or extent[0] > extent[2]
        or extent[1] > extent[3]
    ):
        raise ValueError(f"Invalid computed extent for {label}: {extent}")
    if max(extent[0], aoi[0]) > min(extent[2], aoi[2]) or max(extent[1], aoi[1]) > min(
        extent[3], aoi[3]
    ):
        raise ValueError(f"Computed extent for {label} does not overlap the AOI")
