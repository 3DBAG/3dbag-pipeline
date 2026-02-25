import pytest
from dagster import Output
from bag3d.core.assets.ahn.core import (
    download_ahn_index,
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
    LazFilesConfig,
)
from bag3d.core.assets.ahn.metadata import (
    metadata_table_ahn3,
    metadata_table_ahn4,
    metadata_table_ahn5,
)
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import table_exists


def test_download_ahn_index():
    tile_ids = download_ahn_index()
    assert tile_ids is not None
    assert len(tile_ids) == 1407
    assert tile_ids[list(tile_ids.keys())[0]] is None


def test_download_ahn_index_geometry():
    features = download_ahn_index(with_geom=True)
    assert features is not None
    assert len(features) == 1407
    assert features[list(features.keys())[0]] is not None


@pytest.mark.parametrize(
    "ahn_version",
    (3, 4, 5),
    ids=("ahn3", "ahn4", "ahn5"),
)
def test_get_checksums(ahn_version):
    checksums = get_checksums(URL_LAZ_SHA, ahn_version=ahn_version)
    print(
        f"Found {len(checksums)} checksums for AHN{ahn_version} LAZ files. First five:"
    )
    assert len(checksums) > 0
    for k, sha in list(checksums.items())[:5]:
        assert sha is not None


def test_checksums_for_ahn():
    res = md5_ahn3()
    assert isinstance(res, dict)
    assert len(res) > 0
    for k, sha in list(res.items())[:5]:
        assert sha is not None
    res = md5_ahn4()
    assert isinstance(res, dict)
    assert len(res) > 0
    for k, sha in list(res.items())[:5]:
        assert sha is not None
    res = sha256_ahn5()
    assert isinstance(res, dict)
    assert len(res) > 0
    for k, sha in list(res.items())[:5]:
        assert sha is not None


def test_tile_index_ahn():
    res = tile_index_ahn()
    assert res is not None
    assert isinstance(res, dict)
    assert len(res) == 1407
    assert res[list(res.keys())[0]] is not None


@pytest.mark.slow
def test_laz_files_ahn3(context_ahn, resources_ahn, md5_ahn3_fix, tile_index_ahn_fix):
    config = LazFilesConfig(force_download=False, check_hash=False)
    res = laz_files_ahn3(
        context_ahn,
        config,
        resources_ahn["file_store"],
        md5_ahn3_fix,
        tile_index_ahn_fix,
    )
    assert isinstance(res, Output)
    assert res.value.url is not None
    print(res.value)


@pytest.mark.slow
def test_laz_files_ahn4(context_ahn, resources_ahn, md5_ahn4_fix, tile_index_ahn_fix):
    config = LazFilesConfig(force_download=False, check_hash=False)
    res = laz_files_ahn4(
        context_ahn,
        config,
        resources_ahn["file_store"],
        md5_ahn4_fix,
        tile_index_ahn_fix,
    )
    assert isinstance(res, Output)
    assert res.value.url is not None


@pytest.mark.slow
def test_laz_files_ahn5(
    context_ahn, resources_ahn, sha256_ahn5_fix, tile_index_ahn_fix
):
    config = LazFilesConfig(force_download=False, check_hash=False)
    res = laz_files_ahn5(
        context_ahn,
        config,
        resources_ahn["file_store"],
        sha256_ahn5_fix,
        tile_index_ahn_fix,
    )
    assert isinstance(res, Output)
    assert res.value.url is not None


def test_metadata_table_ahn3(resources_ahn):
    metadata = metadata_table_ahn3(resources_ahn["db_connection"])
    tbl = PostgresTableIdentifier("ahn", "metadata_ahn3")
    assert table_exists(resources_ahn["db_connection"], tbl)
    assert isinstance(metadata, PostgresTableIdentifier)
    assert str(metadata) == f"{tbl.schema}.{tbl.table}"


def test_metadata_table_ahn4(resources_ahn):
    metadata = metadata_table_ahn4(resources_ahn["db_connection"])
    tbl = PostgresTableIdentifier("ahn", "metadata_ahn4")
    assert table_exists(resources_ahn["db_connection"], tbl)
    assert isinstance(metadata, PostgresTableIdentifier)
    assert str(metadata) == f"{tbl.schema}.{tbl.table}"


def test_metadata_table_ahn5(resources_ahn):
    metadata = metadata_table_ahn5(resources_ahn["db_connection"])
    tbl = PostgresTableIdentifier("ahn", "metadata_ahn5")
    assert table_exists(resources_ahn["db_connection"], tbl)
    assert isinstance(metadata, PostgresTableIdentifier)
    assert str(metadata) == f"{tbl.schema}.{tbl.table}"
