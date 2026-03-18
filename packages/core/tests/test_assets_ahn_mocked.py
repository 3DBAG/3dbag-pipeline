from unittest.mock import patch

import dagster as dg

from bag3d.core.assets.ahn.download import (
    get_checksums,
    md5_ahn3,
    tile_index_ahn,
    URL_LAZ_SHA,
)


def test_get_checksums_parses_md5_payload():
    payload = "aaa111  C_01CZ1.LAZ\nbbb222  C_32BZ1.LAZ\n"
    with patch("bag3d.core.assets.ahn.download.download_as_str", return_value=payload):
        checksums = get_checksums(URL_LAZ_SHA, ahn_version=3)

    assert checksums == {"C_01CZ1.LAZ": "aaa111", "C_32BZ1.LAZ": "bbb222"}


def test_tile_index_ahn_uses_downloaded_index():
    index = {
        "01cz1": {
            "AHN3_LAZ": "https://example.com/C_01CZ1.LAZ",
            "geometry": {"type": "Polygon", "coordinates": []},
        }
    }
    with patch("bag3d.core.assets.ahn.download.download_ahn_index", return_value=index):
        assert tile_index_ahn() == index


def test_checksum_asset_materializes_with_mocked_http():
    checksums = {"C_01CZ1.LAZ": "aaa111"}
    with patch("bag3d.core.assets.ahn.download.get_checksums", return_value=checksums):
        result = dg.materialize([md5_ahn3])

    assert result.success
    assert result.output_for_node("md5_ahn3") == checksums
