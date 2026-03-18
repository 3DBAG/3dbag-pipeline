import pytest
from dagster import Output
from bag3d.core.assets.ahn.core import download_ahn_index
from bag3d.core.assets.ahn.download import (
    LAZDownload,
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
from unittest.mock import patch


MOCK_AHN_INDEX = {
    "01cz1": {
        "AHN3_LAZ": "https://example.com/C_01CZ1.LAZ",
        "AHN4_LAZ": "https://example.com/C_01CZ1.LAZ",
        "AHN5_LAZ": "https://example.com/2023_C_01CZ1.LAZ",
        "geometry": {"type": "Polygon", "coordinates": []},
    }
}

MOCK_AHN5_RESPONSE = {
    "features": [
        {
            "properties": {
                "file": "https://example.com/2023_C_01CZ1.LAZ",
                "sha256": "067541da253de88eef78c580a1ff6396c7ec3e3833cc0843a2fac4270b625611",
            }
        }
    ]
}

MOCK_AHN34_RESPONSE = "56c731a1814dd73c79a0a5347f8a04c7  C_01CZ1.LAZ\n"
MOCK_AHN_INDEX_RESPONSE = {
    "features": [
        {
            "properties": {
                "AHN": "01CZ1",
                "AHN3 puntenwolk": "https://example.com/C_01CZ1.LAZ",
                "AHN4 puntenwolk": "https://example.com/C_01CZ1.LAZ",
                "AHN5 puntenwolk": "https://example.com/2023_C_01CZ1.LAZ",
            },
            "geometry": {"type": "Polygon", "coordinates": []},
        }
    ]
}


def test_download_ahn_index_mocked():
    with patch(
        "bag3d.core.assets.ahn.core.requests.get",
        side_effect=[
            type(
                "Response",
                (),
                {
                    "status_code": 200,
                    "json": staticmethod(lambda: MOCK_AHN_INDEX_RESPONSE),
                },
            )()
        ],
    ):
        tile_ids = download_ahn_index()

    assert tile_ids == {"01cz1": None}


def test_download_ahn_index_geometry_mocked():
    with patch(
        "bag3d.core.assets.ahn.core.requests.get",
        side_effect=[
            type(
                "Response",
                (),
                {
                    "status_code": 200,
                    "json": staticmethod(lambda: MOCK_AHN_INDEX_RESPONSE),
                },
            )()
        ],
    ):
        features = download_ahn_index(with_geom=True)

    assert features == MOCK_AHN_INDEX


@pytest.mark.parametrize(
    "ahn_version",
    (3, 4, 5),
    ids=("ahn3", "ahn4", "ahn5"),
)
def test_get_checksums(ahn_version):
    payload = (
        MOCK_AHN34_RESPONSE
        if ahn_version in (3, 4)
        else '{"features": [{"properties": {"file": "https://example.com/2023_C_01CZ1.LAZ", "sha256": "abc"}}]}'
    )
    with patch("bag3d.core.assets.ahn.download.download_as_str", return_value=payload):
        checksums = get_checksums(URL_LAZ_SHA, ahn_version=ahn_version)

    assert len(checksums) == 1
    assert next(iter(checksums.values())) is not None


def test_checksums_for_ahn():
    with patch(
        "bag3d.core.assets.ahn.download.download_as_str",
        side_effect=[
            MOCK_AHN34_RESPONSE,
            MOCK_AHN34_RESPONSE,
            '{"features": [{"properties": {"file": "https://example.com/2023_C_01CZ1.LAZ", "sha256": "abc"}}]}',
        ],
    ):
        assert md5_ahn3() == {"C_01CZ1.LAZ": "56c731a1814dd73c79a0a5347f8a04c7"}
        assert md5_ahn4() == {"C_01CZ1.LAZ": "56c731a1814dd73c79a0a5347f8a04c7"}
        assert sha256_ahn5() == {"2023_C_01CZ1.LAZ": "abc"}


def test_tile_index_ahn():
    with patch(
        "bag3d.core.assets.ahn.download.download_ahn_index", return_value=MOCK_AHN_INDEX
    ):
        assert tile_index_ahn() == MOCK_AHN_INDEX


def _mock_laz_download(tmp_path, filename: str) -> LAZDownload:
    path = tmp_path / filename
    path.write_bytes(b"mock laz content")
    return LAZDownload(
        url=f"https://example.com/{filename}",
        path=path,
        success=True,
        hash_name=None,
        hash_hexdigest=None,
        new=True,
        size=round(path.stat().st_size / 1e6, 2),
    )


def test_laz_files_ahn3(
    context_ahn, resources_ahn, md5_ahn3_fix, tile_index_ahn_fix, tmp_path
):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with patch(
        "bag3d.core.assets.ahn.download.download_ahn_laz",
        return_value=_mock_laz_download(tmp_path, "C_01CZ1.LAZ"),
    ):
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


def test_laz_files_ahn4(
    context_ahn, resources_ahn, md5_ahn4_fix, tile_index_ahn_fix, tmp_path
):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with patch(
        "bag3d.core.assets.ahn.download.download_ahn_laz",
        return_value=_mock_laz_download(tmp_path, "C_01CZ1.LAZ"),
    ):
        res = laz_files_ahn4(
            context_ahn,
            config,
            resources_ahn["file_store"],
            md5_ahn4_fix,
            tile_index_ahn_fix,
        )
    assert isinstance(res, Output)
    assert res.value.url is not None


def test_laz_files_ahn5(
    context_ahn, resources_ahn, sha256_ahn5_fix, tile_index_ahn_fix, tmp_path
):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with patch(
        "bag3d.core.assets.ahn.download.download_ahn_laz",
        return_value=_mock_laz_download(tmp_path, "2023_C_01CZ1.LAZ"),
    ):
        res = laz_files_ahn5(
            context_ahn,
            config,
            resources_ahn["file_store"],
            sha256_ahn5_fix,
            tile_index_ahn_fix,
        )
    assert isinstance(res, Output)
    assert res.value.url is not None
