from unittest.mock import patch

import dagster as dg
import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    Output,
    build_multi_asset_sensor_context,
)

from bag3d.common.testing import build_asset_context_for
from bag3d.core.assets.ahn.core import download_ahn_index
from bag3d.core.assets.ahn.download import (
    LAZDownload,
    LazFilesConfig,
    URL_LAZ_SHA,
    get_checksums,
    laz_files_ahn3,
    laz_files_ahn4,
    laz_files_ahn5,
    md5_ahn3,
    md5_ahn4,
    sha256_ahn5,
)
from bag3d.core.jobs import job_ahn3, job_ahn4, job_ahn5
from bag3d.core.sensors import ahn_checksum_sensor


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
    (3, 4, 5, 6),
    ids=("ahn3", "ahn4", "ahn5", "ahn6"),
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
            '{"features": [{"properties": {"file": "https://example.com/2024_C_01CZ1.LAZ", "sha256": "def"}}]}',
        ],
    ):
        assert md5_ahn3() == {"C_01CZ1.LAZ": "56c731a1814dd73c79a0a5347f8a04c7"}
        assert md5_ahn4() == {"C_01CZ1.LAZ": "56c731a1814dd73c79a0a5347f8a04c7"}
        assert sha256_ahn5() == {"2023_C_01CZ1.LAZ": "abc"}


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


def test_laz_files_ahn3(resources_ahn, md5_ahn3_fix, tile_index_ahn_fix, tmp_path):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with patch(
        "bag3d.core.assets.ahn.download.download_ahn_laz",
        return_value=_mock_laz_download(tmp_path, "C_01CZ1.LAZ"),
    ):
        with build_asset_context_for(laz_files_ahn3, partition_key="01cz1") as context:
            res = laz_files_ahn3(
                context,
                config,
                resources_ahn["file_store"],
                md5_ahn3_fix,
                tile_index_ahn_fix,
            )
    assert isinstance(res, Output)
    assert res.value.url is not None


def test_laz_files_ahn4(resources_ahn, md5_ahn4_fix, tile_index_ahn_fix, tmp_path):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with patch(
        "bag3d.core.assets.ahn.download.download_ahn_laz",
        return_value=_mock_laz_download(tmp_path, "C_01CZ1.LAZ"),
    ):
        with build_asset_context_for(laz_files_ahn4, partition_key="01cz1") as context:
            res = laz_files_ahn4(
                context,
                config,
                resources_ahn["file_store"],
                md5_ahn4_fix,
                tile_index_ahn_fix,
            )
    assert isinstance(res, Output)
    assert res.value.url is not None


def test_laz_files_ahn5(resources_ahn, sha256_ahn5_fix, tile_index_ahn_fix, tmp_path):
    config = LazFilesConfig(force_download=False, check_hash=False)
    with patch(
        "bag3d.core.assets.ahn.download.download_ahn_laz",
        return_value=_mock_laz_download(tmp_path, "2023_C_01CZ1.LAZ"),
    ):
        with build_asset_context_for(laz_files_ahn5, partition_key="01cz1") as context:
            res = laz_files_ahn5(
                context,
                config,
                resources_ahn["file_store"],
                sha256_ahn5_fix,
                tile_index_ahn_fix,
            )
    assert isinstance(res, Output)
    assert res.value.url is not None


def test_laz_files_ahn3_retries_after_checksum_failure(
    resources_ahn, md5_ahn3_fix, tile_index_ahn_fix, tmp_path
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
    ):
        with build_asset_context_for(laz_files_ahn3, partition_key="01cz1") as context:
            res = laz_files_ahn3(
                context,
                config,
                resources_ahn["file_store"],
                md5_ahn3_fix,
                tile_index_ahn_fix,
            )

    assert isinstance(res, Output)
    assert download_mock.call_count == 2
    assert validate_mock.call_count == 2
    assert len(downloads) == 2
    assert downloads[0].path == downloads[1].path
    assert res.value.path == downloads[1].path


def test_ahn_checksum_sensor_skips_unknown_filename_mapping():
    defs = dg.Definitions(jobs=[job_ahn3, job_ahn4, job_ahn5])
    sensor = ahn_checksum_sensor(dg.DefaultSensorStatus.STOPPED)
    initial_cursor = '{"ahn3": {"C_01CZ1.LAZ": "aaa111"}}'

    with dg.DagsterInstance.ephemeral() as inst:
        inst.report_runless_asset_event(
            AssetMaterialization(asset_key=AssetKey(["ahn", "md5_ahn3"]))
        )
        with (
            patch(
                "bag3d.core.sensors.get_checksums",
                return_value={"C_UNKNOWN.LAZ": "bbb222"},
            ),
            patch("bag3d.core.sensors.download_ahn_index", return_value=MOCK_AHN_INDEX),
        ):
            ctx = build_multi_asset_sensor_context(
                monitored_assets=[
                    AssetKey(["ahn", "md5_ahn3"]),
                    AssetKey(["ahn", "md5_ahn4"]),
                    AssetKey(["ahn", "sha256_ahn5"]),
                ],
                instance=inst,
                cursor=initial_cursor,
                definitions=defs,
            )
            result = sensor(ctx)

    assert isinstance(result, dg.SkipReason)
    assert "No checksum changes detected" in (result.skip_message or "")
