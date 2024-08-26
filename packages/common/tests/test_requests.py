import pytest
from bag3d.common.utils.requests import (get_extract_download_link, get_metadata, download_as_str, download_file, download_extract)

URL_TEST = "https://gist.githubusercontent.com/fwrite/6bb4ad23335c861f9f3162484e57a112/raw/ee5274c7c6cf42144d569e303cf93bcede3e2da1/AHN4.md5"
URL_TEST_ERROR = "not-a-url"

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


def test_download_as_str():
    res = download_as_str(url=URL_TEST)
    print(res.split('\n', 1)[0])
    assert res.split('\n', 1)[0] == '56c731a1814dd73c79a0a5347f8a04c7  C_01CZ1.LAZ'


def test_download_file(tmp_path):
    res = download_file(url=URL_TEST, target_path=tmp_path / "test.md5")
    assert res == tmp_path / "test.md5"


def test_download_file_2(tmp_path):
    res = download_file(url=URL_TEST, target_path=tmp_path)
    print(res)
    assert res == tmp_path / "AHN4.md5"


def test_download_extra():
    metadata = download_extract(
        dataset="top10nl",
        url_api="https://api.pdok.nl/brt/top10nl/download/v1_0",
        featuretypes=context.op_config["featuretypes"],
        data_format="gml",
        geofilter=context.op_config.get("geofilter"),
        download_dir=context.resources.file_store.data_dir
    )
