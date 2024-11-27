import pytest
from bag3d.core.assets.ahn.core import (
    ahn_laz_dir,
    download_ahn_index,
    generate_grid,
    tile_index_origin,
)
from bag3d.core.assets.ahn.download import (
    URL_LAZ_SHA,
    get_checksums,
    laz_files_ahn3,
    laz_files_ahn4,
    laz_files_ahn5,
    md5_ahn3,
    md5_ahn4,
    sha256_ahn5,
    tile_index_ahn,
)
from bag3d.core.assets.ahn.metadata import (
    metadata_table_ahn3,
    metadata_table_ahn4,
    metadata_table_ahn5,
)
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import table_exists


def test_tile_index_origin():
    minx, miny, maxx, maxy = tile_index_origin()
    assert minx == pytest.approx(9999.99998)
    assert miny == pytest.approx(306250.00034)
    assert maxx == pytest.approx(280000.00023)
    assert maxy == pytest.approx(625000.00053)


def test_download_ahn_index():
    tile_ids = download_ahn_index()
    assert len(tile_ids) == 1406
    assert tile_ids[list(tile_ids.keys())[0]] is None


def test_download_ahn_index_geometry():
    features = download_ahn_index(with_geom=True)
    assert len(features) == 1406
    assert features[list(features.keys())[0]] is not None


def test_generate_grid():
    PDOK_TILE_INDEX_BBOX = (13000, 306250, 279000, 616250)
    grid = generate_grid(PDOK_TILE_INDEX_BBOX, 200)
    assert grid == ((13000, 306250, 279000, 616250), 1330, 1550)


@pytest.mark.parametrize(
    "url",
    (URL_LAZ_SHA["ahn3"], URL_LAZ_SHA["ahn4"], URL_LAZ_SHA["ahn5"]),
    ids=("ahn3", "ahn4", "ahn5"),
)
def test_get_checksums(url):
    checksums = get_checksums(url)
    assert len(checksums) > 0
    for k, sha in list(checksums.items())[:5]:
        assert sha is not None


def test_checksums_for_ahn(context):
    res = md5_ahn3(context)
    assert len(res) > 0
    for k, sha in list(res.items())[:5]:
        assert sha is not None
    res = md5_ahn4(context)
    assert len(res) > 0
    for k, sha in list(res.items())[:5]:
        assert sha is not None
    res = sha256_ahn5(context)
    assert len(res) > 0
    for k, sha in list(res.items())[:5]:
        assert sha is not None


def test_tile_index_ahn(context):
    res = tile_index_ahn(context)
    assert len(res) == 1406
    assert res[list(res.keys())[0]] is not None


@pytest.mark.slow
def test_laz_files_ahn3(context, md5_ahn3_fix, tile_index_ahn_fix):
    laz_dir = ahn_laz_dir(context.resources.file_store.file_store.data_dir, 3)
    laz_dir.mkdir(exist_ok=True, parents=True)
    res = laz_files_ahn3(context, md5_ahn3_fix, tile_index_ahn_fix)
    assert res.value.url is not None
    assert res is not None
    print(res.value)


@pytest.mark.slow
def test_laz_files_ahn4(context, md5_ahn4_fix, tile_index_ahn_fix):
    laz_dir = ahn_laz_dir(context.resources.file_store.file_store.data_dir, 4)
    laz_dir.mkdir(exist_ok=True, parents=True)
    res = laz_files_ahn4(context, md5_ahn4_fix, tile_index_ahn_fix)
    assert res.value.url is not None
    assert res is not None


@pytest.mark.slow
def test_laz_files_ahn5(context, sha256_ahn5_fix, tile_index_ahn_fix):
    laz_dir = ahn_laz_dir(context.resources.file_store.file_store.data_dir, 5)
    laz_dir.mkdir(exist_ok=True, parents=True)
    res = laz_files_ahn5(context, sha256_ahn5_fix, tile_index_ahn_fix)
    assert res.value.url is not None
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


def test_metadata_table_ahn5(context):
    metadata = metadata_table_ahn5(context)
    tbl = PostgresTableIdentifier("ahn", "metadata_ahn5")
    assert table_exists(context, tbl)
    assert isinstance(metadata, PostgresTableIdentifier)
    assert str(metadata) == f"{tbl.schema}.{tbl.table}"
