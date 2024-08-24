import pytest
from bag3d.common.utils.requests import get_extract_download_link, get_metadata


def test_get_metadata():
    res = get_metadata("https://api.pdok.nl/brt/top10nl/download/v1_0/dataset")
    assert res


@pytest.mark.slow
@pytest.mark.parametrize(
    "geofilter",
    ("testarea",),
    ids=[
        "testarea",
    ],
)
def test_download_link(wkt_testarea, geofilter):
    """Can we get a valid download link with a WKT geofilter and also with a None,
    which should download the whole NL?"""
    if geofilter == "testarea":
        geofilter = wkt_testarea
    res = get_extract_download_link(
        url="https://api.pdok.nl/brt/top10nl/download/v1_0/full/custom",
        featuretypes=[
            "gebouw",
        ],
        data_format="gml",
        geofilter=geofilter,
    )
    assert res
