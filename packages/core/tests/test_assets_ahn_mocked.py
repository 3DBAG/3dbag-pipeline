from unittest.mock import patch

from bag3d.core.assets.ahn.download import (
    get_checksums,
    URL_LAZ_SHA,
)


def test_get_checksums_parses_md5_payload():
    payload = "aaa111  C_01CZ1.LAZ\nbbb222  C_32BZ1.LAZ\n"
    with patch("bag3d.core.assets.ahn.download.download_as_str", return_value=payload):
        checksums = get_checksums(URL_LAZ_SHA, ahn_version=3)

    assert checksums == {"C_01CZ1.LAZ": "aaa111", "C_32BZ1.LAZ": "bbb222"}
