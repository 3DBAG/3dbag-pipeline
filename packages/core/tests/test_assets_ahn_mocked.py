from unittest.mock import patch

import pytest
from dagster import Failure

from bag3d.core.assets.ahn.download import (
    URL_LAZ_SHA,
    download_ahn_laz,
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


def test_download_ahn_laz_raises_on_download_failure(tmp_path):
    """A LAZ that fails to download on every retry raises a Failure."""
    fpath = tmp_path / "C_01CZ1.LAZ"
    with (
        patch("bag3d.core.assets.ahn.download.download_file", return_value=None),
        patch("bag3d.core.assets.ahn.download.time.sleep"),
        pytest.raises(Failure, match="Downloading failed!"),
    ):
        download_ahn_laz(
            fpath=fpath,
            url_laz="https://example.com/C_01CZ1.LAZ",
            verify_ssl=False,
        )


@pytest.mark.parametrize(
    "url_laz",
    (None, "", "ftp://example.com/C_01CZ1.LAZ"),
    ids=("none", "empty", "non_http"),
)
def test_download_ahn_laz_raises_on_invalid_url(tmp_path, url_laz):
    """A missing or malformed URL fails clearly without a network request."""
    with pytest.raises(Failure, match="No valid download URL"):
        download_ahn_laz(fpath=tmp_path / "C_01CZ1.LAZ", url_laz=url_laz)
