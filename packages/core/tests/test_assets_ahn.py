import pytest
from bag3d.core.assets.ahn.core import (
    ahn_laz_dir,
    download_ahn_index,
    generate_grid,
)
from bag3d.core.assets.ahn.download import (
    URL_LAZ_SHA,
    get_md5_pdok,
    laz_files_ahn3,
    laz_files_ahn4,
    md5_pdok_ahn3,
    md5_pdok_ahn4,
    tile_index_pdok,
)
from bag3d.core.assets.ahn.metadata import metadata_table_ahn3, metadata_table_ahn4
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import table_exists


def test_download_ahn_index():
    tile_ids = download_ahn_index()
    assert len(tile_ids) == 1407
    assert tile_ids[list(tile_ids.keys())[0]] is None


def test_download_ahn_index_geometry():
    features = download_ahn_index(with_geom=True)
    assert len(features) == 1407
    assert features[list(features.keys())[0]] is not None


def test_generate_grid():
    PDOK_TILE_INDEX_BBOX = (13000, 306250, 279000, 616250)
    grid = generate_grid(PDOK_TILE_INDEX_BBOX, 200)
    assert grid == ((13000, 306250, 279000, 616250), 1330, 1550)


@pytest.mark.parametrize(
    "url", (URL_LAZ_SHA["ahn3"], URL_LAZ_SHA["ahn4"]), ids=("ahn3", "ahn4")
)
def test_get_md5_pdok(url):
    md5_pdok = get_md5_pdok(url)
    assert len(md5_pdok) > 0
    for k, sha in list(md5_pdok.items())[:5]:
        assert sha is not None


def test_md5_pdok_ahn(context):
    res = md5_pdok_ahn3(context)
    assert len(res) > 0
    for k, sha in list(res.items())[:5]:
        assert sha is not None
    res = md5_pdok_ahn4(context)
    assert len(res) > 0
    for k, sha in list(res.items())[:5]:
        assert sha is not None


def test_tile_index_pdok(context):
    res = tile_index_pdok(context)
    assert len(res) == 1407
    assert res[list(res.keys())[0]] is not None

@pytest.mark.slow
def test_laz_files_ahn3(context, md5_pdok_ahn3_fix, tile_index_ahn3_pdok_fix):
    laz_dir = ahn_laz_dir(context.resources.file_store.data_dir, 3)
    laz_dir.mkdir(exist_ok=True, parents=True)
    res = laz_files_ahn3(context, md5_pdok_ahn3_fix, tile_index_ahn3_pdok_fix)
    assert res is not None
    print(res.value)


@pytest.mark.slow
def test_laz_files_ahn4(context, md5_pdok_ahn4_fix, tile_index_ahn4_pdok_fix):
    laz_dir = ahn_laz_dir(context.resources.file_store.data_dir, 4)
    laz_dir.mkdir(exist_ok=True, parents=True)
    res = laz_files_ahn4(context, md5_pdok_ahn4_fix, tile_index_ahn4_pdok_fix)
    assert res is not None


def test_metadata_table_ahn3(context):
    metadata = metadata_table_ahn3(context)
    tbl = PostgresTableIdentifier("ahn", "metadata_ahn3")
    assert table_exists(context, tbl)
    assert isinstance(metadata, PostgresTableIdentifier)
    assert str(metadata) == f"{tbl.schema}.{tbl.table}"


def test_metadata_table_ahn4(context):
    metadata = metadata_table_ahn4(context)
    tbl = PostgresTableIdentifier("ahn", "metadata_ahn4")
    assert table_exists(context, tbl)
    assert isinstance(metadata, PostgresTableIdentifier)
    assert str(metadata) == f"{tbl.schema}.{tbl.table}"
