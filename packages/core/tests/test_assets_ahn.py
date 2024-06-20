import pytest
from bag3d.core.assets.ahn.core import download_ahn_index_esri, generate_grid
from bag3d.core.assets.ahn.download import URL_LAZ_SHA, get_md5_pdok


@pytest.mark.parametrize("ahn_version", (3, 4), ids=("ahn3", "ahn4"))
def test_download_ahn_index_esri(ahn_version):
    tile_ids = download_ahn_index_esri(ahn_version)
    assert len(tile_ids) > 0
    assert tile_ids[list(tile_ids.keys())[0]] is None


@pytest.mark.parametrize("ahn_version", (3, 4), ids=("ahn3", "ahn4"))
def test_download_ahn_index_esri_geometry(ahn_version):
    features = download_ahn_index_esri(ahn_version, with_geom=True)
    assert len(features) > 0
    assert features[list(features.keys())[0]] is not None


@pytest.mark.parametrize("url", (
        URL_LAZ_SHA["ahn3"],
        URL_LAZ_SHA["ahn4"]
), ids=("ahn3", "ahn4"))
def test_get_md5_pdok(url):
    md5_pdok = get_md5_pdok(url)
    assert len(md5_pdok) > 0
    for k, sha in list(md5_pdok.items())[:5]:
        assert sha is not None
        print(k, sha)

def test_generate_grid():
    PDOK_TILE_INDEX_BBOX = (13000, 306250, 279000, 616250)
    grid = generate_grid(PDOK_TILE_INDEX_BBOX, 200)
    assert grid == ((13000, 306250, 279000, 616250), 1330, 1550)
