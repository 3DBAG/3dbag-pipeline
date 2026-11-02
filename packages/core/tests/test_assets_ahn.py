from unittest.mock import patch

import pytest
from bag3d.common.testing import build_asset_context_for
from dagster import (
    Failure,
    IntMetadataValue,
    Output,
)

from bag3d.core.assets.ahn.core import download_ahn_pc_index
from bag3d.core.assets.ahn.download import (
    LAZDownload,
    LazFilesConfig,
    laz_files_ahn3,
    laz_files_ahn4,
    laz_files_ahn5,
    laz_files_ahn6,
)

MOCK_AHN_INDEX = {
    "01cz1": {
        "url": "https://example.com/C_01CZ1.LAZ",
        "sha256": "56c731a1814dd73c79a0a5347f8a04c7",
        "geometry": {"type": "Polygon", "coordinates": []},
    }
}

MOCK_AHN_PC_RESPONSE = {
    "features": [
        {
            "properties": {
                "file": "https://example.com/C_01CZ1.LAZ",
                "sha256": "56c731a1814dd73c79a0a5347f8a04c7",
            },
            "geometry": {"type": "Polygon", "coordinates": []},
        }
    ]
}


def _mock_response(payload):
    return type(
        "Response",
        (),
        {
            "status_code": 200,
            "raise_for_status": staticmethod(lambda: None),
            "json": staticmethod(lambda: payload),
        },
    )()


def test_download_ahn_pc_index_mocked():
    with patch(
        "bag3d.core.assets.ahn.core.requests.get",
        return_value=_mock_response(MOCK_AHN_PC_RESPONSE),
    ):
        tiles = download_ahn_pc_index(3, with_geom=False)

    assert tiles == {
        "01cz1": {
            "url": "https://example.com/C_01CZ1.LAZ",
            "sha256": "56c731a1814dd73c79a0a5347f8a04c7",
            "geometry": None,
        }
    }


def test_download_ahn_pc_index_geometry_mocked():
    with patch(
        "bag3d.core.assets.ahn.core.requests.get",
        return_value=_mock_response(MOCK_AHN_PC_RESPONSE),
    ):
        tiles = download_ahn_pc_index(3, with_geom=True)

    assert tiles == MOCK_AHN_INDEX


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


def test_laz_files_ahn3(resources_ahn, tile_index_ahn3_fix, tmp_path):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with (
        patch(
            "bag3d.core.assets.ahn.download.download_ahn_laz",
            return_value=_mock_laz_download(tmp_path, "C_01CZ1.LAZ"),
        ),
        build_asset_context_for(laz_files_ahn3, partition_key="01cz1") as context,
    ):
        res = laz_files_ahn3(
            context,
            config,
            resources_ahn["file_store"],
            tile_index_ahn3_fix,
        )
    assert isinstance(res, Output)
    assert res.value.url is not None


def test_laz_files_ahn4(resources_ahn, tile_index_ahn4_fix, tmp_path):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with (
        patch(
            "bag3d.core.assets.ahn.download.download_ahn_laz",
            return_value=_mock_laz_download(tmp_path, "C_01CZ1.LAZ"),
        ),
        build_asset_context_for(laz_files_ahn4, partition_key="01cz1") as context,
    ):
        res = laz_files_ahn4(
            context,
            config,
            resources_ahn["file_store"],
            tile_index_ahn4_fix,
        )
    assert isinstance(res, Output)
    assert res.value.url is not None


def test_laz_files_ahn5(resources_ahn, tile_index_ahn5_fix, tmp_path):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with (
        patch(
            "bag3d.core.assets.ahn.download.download_ahn_laz",
            return_value=_mock_laz_download(tmp_path, "2023_C_01CZ1.LAZ"),
        ),
        build_asset_context_for(laz_files_ahn5, partition_key="01cz1") as context,
    ):
        res = laz_files_ahn5(
            context,
            config,
            resources_ahn["file_store"],
            tile_index_ahn5_fix,
        )
    assert isinstance(res, Output)
    assert res.value.url is not None


def test_laz_files_ahn3_retries_after_checksum_failure(
    resources_ahn, tile_index_ahn3_fix, tmp_path
):
    config = LazFilesConfig(force_download=False, check_hash=True)
    downloads: list[LAZDownload] = []

    def fake_download_ahn_laz(*, fpath, url_laz, verify_ssl, force_download=False):
        fpath.write_bytes(b"mock laz content")
        download = LAZDownload(
            url=url_laz,
            path=fpath,
            success=True,
            hash_name=None,
            hash_hexdigest=None,
            new=True,
            size=round(fpath.stat().st_size / 1e6, 2),
        )
        downloads.append(download)
        return download

    with (
        patch(
            "bag3d.core.assets.ahn.download.download_ahn_laz",
            side_effect=fake_download_ahn_laz,
        ) as download_mock,
        patch.object(LAZDownload, "compute_sha"),
        patch.object(
            LAZDownload, "validate", side_effect=[False, True]
        ) as validate_mock,
        build_asset_context_for(laz_files_ahn3, partition_key="01cz1") as context,
    ):
        res = laz_files_ahn3(
            context,
            config,
            resources_ahn["file_store"],
            tile_index_ahn3_fix,
        )

    assert isinstance(res, Output)
    assert download_mock.call_count == 2
    assert validate_mock.call_count == 2
    assert len(downloads) == 2
    assert downloads[0].path == downloads[1].path
    assert res.value.path == downloads[1].path


def test_laz_files_ahn3_warns_on_persistent_checksum_mismatch(
    resources_ahn, tile_index_ahn3_fix, tmp_path
):
    """A corrupt LAZ whose checksum never matches still returns (warns, no fail)."""
    config = LazFilesConfig(force_download=False, check_hash=True)
    downloads: list[LAZDownload] = []

    def fake_download_ahn_laz(*, fpath, url_laz, verify_ssl, force_download=False):
        fpath.write_bytes(b"mock laz content")
        download = LAZDownload(
            url=url_laz,
            path=fpath,
            success=True,
            hash_name=None,
            hash_hexdigest=None,
            new=True,
            size=round(fpath.stat().st_size / 1e6, 2),
        )
        downloads.append(download)
        return download

    with (
        patch(
            "bag3d.core.assets.ahn.download.download_ahn_laz",
            side_effect=fake_download_ahn_laz,
        ) as download_mock,
        patch.object(LAZDownload, "compute_sha"),
        patch.object(
            LAZDownload, "validate", side_effect=[False, False]
        ) as validate_mock,
        patch("bag3d.core.assets.ahn.download.logger.warning") as warning_mock,
        build_asset_context_for(laz_files_ahn3, partition_key="01cz1") as context,
    ):
        res = laz_files_ahn3(
            context,
            config,
            resources_ahn["file_store"],
            tile_index_ahn3_fix,
        )

    assert isinstance(res, Output)
    assert download_mock.call_count == 2
    assert validate_mock.call_count == 2
    assert len(downloads) == 2
    warning_mock.assert_called()
    assert any(
        "Checksum failed" in str(call.args[0]) for call in warning_mock.call_args_list
    )


@pytest.mark.parametrize(
    "asset_fn",
    [laz_files_ahn3, laz_files_ahn4, laz_files_ahn5],
)
@pytest.mark.parametrize("url_value", [None, ""], ids=("none", "empty"))
def test_laz_files_raises_when_url_none_or_empty(
    resources_ahn, tmp_path, asset_fn, url_value
):
    """A tile with no AHN data (None/empty URL in the index) fails with a clear,
    distinguishable message rather than an opaque AttributeError on ``.split()``."""
    tile_index = {"01cz1": {"url": url_value}}
    with (
        build_asset_context_for(asset_fn, partition_key="01cz1") as context,
        pytest.raises(Failure, match="None or empty"),
    ):
        asset_fn(
            context,
            LazFilesConfig(),
            resources_ahn["file_store"],
            tile_index,
        )


def test_laz_files_ahn6_skips_tile_with_null_url(resources_ahn, tmp_path):
    """AHN6 is graceful for a null/empty URL: it logs a warning, counts the tile
    as failed, and continues -- it does not raise (unlike laz_files_ahn3/4/5)."""
    tiles = ["150000_460000", "150000_461000", "150000_462000"]
    tile_index = {
        t: {"url": f"https://example.com/{t}.LAZ", "geometry": None} for t in tiles
    }
    null_tile = tiles[0]
    tile_index[null_tile] = {"url": None, "geometry": None}

    config = LazFilesConfig(check_hash=False)

    def fake_download_ahn_laz(*, fpath, url_laz, verify_ssl, force_download=False):
        fpath.write_bytes(b"mock laz content")
        return LAZDownload(
            url=url_laz,
            path=fpath,
            success=True,
            hash_name=None,
            hash_hexdigest=None,
            new=True,
            size=round(fpath.stat().st_size / 1e6, 2),
        )

    with (
        patch("bag3d.core.assets.ahn.download.tiles_in_batch", return_value=tiles),
        patch(
            "bag3d.core.assets.ahn.download.download_ahn_laz",
            side_effect=fake_download_ahn_laz,
        ),
        patch.object(LAZDownload, "compute_sha"),
        patch("bag3d.core.assets.ahn.download.logger.warning") as warning_mock,
        build_asset_context_for(
            laz_files_ahn6, partition_key="150000_460000"
        ) as context,
    ):
        res = laz_files_ahn6(context, config, resources_ahn["file_store"], tile_index)

    assert isinstance(res, Output)
    failed = res.metadata["failed"]
    assert isinstance(failed, IntMetadataValue)
    assert failed.value is not None
    assert failed.value >= 1
    assert any(
        "not found in tile index" in str(call.args[0])
        for call in warning_mock.call_args_list
    )
