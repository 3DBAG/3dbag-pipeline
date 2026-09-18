from unittest.mock import patch

from bag3d.core.assets.ahn.download import (
    URL_LAZ_SHA,
    get_checksums,
)


def test_get_checksums_parses_sha256_payload():
    payload = (
        '{"type": "FeatureCollection", "features": ['
        '{"type": "Feature", "properties": {'
        '"file": "https://example.com/C_01CZ1.LAZ", "sha256": "aaa111"}},'
        '{"type": "Feature", "properties": {'
        '"file": "https://example.com/C_32BZ1.LAZ", "sha256": "bbb222"}}'
        "]}"
    )
    with patch("bag3d.core.assets.ahn.download.download_as_str", return_value=payload):
        checksums = get_checksums(URL_LAZ_SHA, ahn_version=3)

    assert checksums == {"C_01CZ1.LAZ": "aaa111", "C_32BZ1.LAZ": "bbb222"}
