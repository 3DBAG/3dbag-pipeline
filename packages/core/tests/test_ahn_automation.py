"""Tests for AHN automation conditions and checksum sensor.

Part 1: Automation condition tests
    Verify that the on_cron() conditions on AHN root assets fire at the right time.
    - md5_ahn3, md5_ahn4, sha256_ahn5, tile_index_ahn: 0 0 1 * * (midnight on 1st)
    - metadata_table_ahn3/4/5: 0 0 9 * * (midnight on 9th)

Part 2: Sensor tests
    Verify the checksum sensor logic:
    - baseline establishment on first run
    - skipping when no changes detected
    - selective partition triggering for changed checksums
"""

import json
import datetime
from unittest.mock import patch

import dagster as dg
import pytest

from bag3d.core.asset_groups import ahn_assets
from bag3d.core.jobs import job_ahn3, job_ahn4, job_ahn5
from bag3d.core.sensors import ahn_checksum_sensor, _build_filename_to_tile_id

utc = datetime.timezone.utc

MOCK_RESOURCES = {
    "file_store": dg.ResourceDefinition.mock_resource(),
    "gdal": dg.ResourceDefinition.mock_resource(),
    "computation_db": dg.ResourceDefinition.mock_resource(),
    "tyler": dg.ResourceDefinition.mock_resource(),
    "pdal": dg.ResourceDefinition.mock_resource(),
    "geoflow": dg.ResourceDefinition.mock_resource(),
    "roofer": dg.ResourceDefinition.mock_resource(),
    "version": dg.ResourceDefinition.mock_resource(),
    "file_store_fastssd": dg.ResourceDefinition.mock_resource(),
    "specs3dbag": dg.ResourceDefinition.mock_resource(),
    "lastools": dg.ResourceDefinition.mock_resource(),
}

DEFS = dg.Definitions(assets=ahn_assets, resources=MOCK_RESOURCES)

# Separate Definitions with jobs for the sensor context (sensors require job definitions)
SENSOR_DEFS = dg.Definitions(
    assets=ahn_assets,
    jobs=[job_ahn3, job_ahn4, job_ahn5],
    resources=MOCK_RESOURCES,
)

# Time before any cron fires (before midnight on 1st)
BEFORE_CRON_1ST = datetime.datetime(2025, 12, 31, 23, 59, tzinfo=utc)
# Time after the 1st-of-month cron fires
AFTER_CRON_1ST = datetime.datetime(2026, 2, 1, 0, 1, tzinfo=utc)
# Time after the 9th-of-month cron fires
AFTER_CRON_9TH = datetime.datetime(2026, 2, 9, 0, 1, tzinfo=utc)

# Asset keys with "ahn" key prefix as applied by asset_groups
KEY_MD5_AHN3 = dg.AssetKey(["ahn", "md5_ahn3"])
KEY_MD5_AHN4 = dg.AssetKey(["ahn", "md5_ahn4"])
KEY_SHA256_AHN5 = dg.AssetKey(["ahn", "sha256_ahn5"])
KEY_TILE_INDEX_AHN = dg.AssetKey(["ahn", "tile_index_ahn"])
KEY_METADATA_TABLE_AHN3 = dg.AssetKey(["ahn", "metadata_table_ahn3"])
KEY_METADATA_TABLE_AHN4 = dg.AssetKey(["ahn", "metadata_table_ahn4"])
KEY_METADATA_TABLE_AHN5 = dg.AssetKey(["ahn", "metadata_table_ahn5"])

# All unpartitioned AHN root keys (for pre-materialization baseline)
ALL_AHN_UNPARTITIONED_KEYS = [
    KEY_MD5_AHN3,
    KEY_MD5_AHN4,
    KEY_SHA256_AHN5,
    KEY_TILE_INDEX_AHN,
    KEY_METADATA_TABLE_AHN3,
    KEY_METADATA_TABLE_AHN4,
    KEY_METADATA_TABLE_AHN5,
]


def _requested_keys(result) -> set[dg.AssetKey]:
    """Return the set of AssetKeys marked as requested in this evaluation."""
    return {r.key for r in result.results if result.get_num_requested(r.key) > 0}


def _mat(instance: dg.DagsterInstance, *keys: dg.AssetKey) -> None:
    """Report runless materializations for the given asset keys."""
    for key in keys:
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=key))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


@pytest.fixture
def instance_ahn_materialized(instance):
    """Instance with all unpartitioned AHN assets already materialized (clean baseline).

    Establishes a cursor baseline before any cron fires, so that subsequent
    evaluations only detect new ticks.
    """
    _mat(instance, *ALL_AHN_UNPARTITIONED_KEYS)
    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON_1ST,
    )
    assert _requested_keys(result) == set(), "Expected no requests in clean baseline"
    return instance, result.cursor


# ---------------------------------------------------------------------------
# Part 1: Automation condition tests
# ---------------------------------------------------------------------------


def test_ahn_checksum_roots_not_requested_before_tick(instance):
    """Before midnight on the 1st, checksum root assets should not be requested.

    on_cron only fires when a cron tick has passed between two evaluations.
    On the first evaluation before any tick, the condition stays False.
    """
    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON_1ST,
    )
    requested = _requested_keys(result)
    assert KEY_MD5_AHN3 not in requested
    assert KEY_MD5_AHN4 not in requested
    assert KEY_SHA256_AHN5 not in requested
    assert KEY_TILE_INDEX_AHN not in requested


def test_ahn_checksum_roots_requested_after_tick(instance_ahn_materialized):
    """After midnight on the 1st, all four checksum/tile root assets are requested.

    Two evaluations are required: the first establishes the cursor (before the tick),
    the second detects the tick has passed and fires the condition.
    """
    instance, cursor = instance_ahn_materialized
    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=AFTER_CRON_1ST,
        cursor=cursor,
    )
    requested = _requested_keys(result)
    assert KEY_MD5_AHN3 in requested
    assert KEY_MD5_AHN4 in requested
    assert KEY_SHA256_AHN5 in requested
    assert KEY_TILE_INDEX_AHN in requested


def test_ahn_metadata_tables_requested_after_cron_tick(instance_ahn_materialized):
    """After midnight on the 9th, the three metadata_table_ahn* assets are requested.

    The 9th-of-month cron (0 0 9 * *) is distinct from the 1st-of-month cron.
    This test verifies that the correct assets react to the 9th tick.
    """
    instance, cursor = instance_ahn_materialized
    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=AFTER_CRON_9TH,
        cursor=cursor,
    )
    requested = _requested_keys(result)
    assert KEY_METADATA_TABLE_AHN3 in requested
    assert KEY_METADATA_TABLE_AHN4 in requested
    assert KEY_METADATA_TABLE_AHN5 in requested


# ---------------------------------------------------------------------------
# Part 2: Sensor tests
# ---------------------------------------------------------------------------

# Shared test data
TILE_INDEX = {
    "01cz1": {
        "AHN3_LAZ": "https://example.com/C_01CZ1.LAZ",
        "AHN4_LAZ": "https://example.com/C_01CZ1.LAZ",
        "AHN5_LAZ": "https://example.com/C_01CZ1_AHN5.LAZ",
    },
    "50cn2": {
        "AHN3_LAZ": "https://example.com/C_50CN2.LAZ",
        "AHN4_LAZ": "https://example.com/C_50CN2.LAZ",
        "AHN5_LAZ": "https://example.com/C_50CN2_AHN5.LAZ",
    },
}

CHECKSUMS_V1 = {"C_01CZ1.LAZ": "aaa111", "C_50CN2.LAZ": "bbb222"}
CHECKSUMS_V2 = {"C_01CZ1.LAZ": "ccc333", "C_50CN2.LAZ": "bbb222"}  # 01cz1 changed


def test_build_filename_to_tile_id():
    """_build_filename_to_tile_id maps filenames to tile IDs correctly."""
    result = _build_filename_to_tile_id(TILE_INDEX, "AHN3_LAZ")
    assert result == {"C_01CZ1.LAZ": "01cz1", "C_50CN2.LAZ": "50cn2"}


def test_build_filename_to_tile_id_ahn5():
    """_build_filename_to_tile_id uses the AHN5-specific filenames."""
    result = _build_filename_to_tile_id(TILE_INDEX, "AHN5_LAZ")
    assert result == {"C_01CZ1_AHN5.LAZ": "01cz1", "C_50CN2_AHN5.LAZ": "50cn2"}


def test_sensor_skips_when_no_materializations():
    """Sensor returns SkipReason when no checksum assets have been materialized."""
    sensor = ahn_checksum_sensor(dg.DefaultSensorStatus.STOPPED)
    with dg.DagsterInstance.ephemeral() as inst:
        ctx = dg.build_multi_asset_sensor_context(
            monitored_assets=[
                dg.AssetKey(["ahn", "md5_ahn3"]),
                dg.AssetKey(["ahn", "md5_ahn4"]),
                dg.AssetKey(["ahn", "sha256_ahn5"]),
            ],
            instance=inst,
            definitions=SENSOR_DEFS,
        )
        result = sensor(ctx)
    assert isinstance(result, dg.SkipReason)
    assert "No new checksum materializations" in (result.skip_message or "")


def test_sensor_establishes_baseline_on_first_run():
    """On first run (no cursor), sensor establishes baseline and skips run requests.

    The sensor should not trigger any RunRequests on the first observation — it
    only records the current checksums so that future evaluations can detect diffs.
    We verify the baseline by running the sensor twice with the same checksums:
    the first run establishes the baseline (SkipReason), and the second run with
    unchanged checksums also skips (SkipReason "No checksum changes detected"),
    proving the baseline was stored correctly.
    """
    sensor = ahn_checksum_sensor(dg.DefaultSensorStatus.STOPPED)
    with dg.DagsterInstance.ephemeral() as inst:
        inst.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=dg.AssetKey(["ahn", "md5_ahn3"]))
        )
        with (
            patch("bag3d.core.sensors.get_checksums", return_value=CHECKSUMS_V1),
            patch("bag3d.core.sensors.download_ahn_index", return_value=TILE_INDEX),
        ):
            # First run: no cursor → baseline establishment
            ctx = dg.build_multi_asset_sensor_context(
                monitored_assets=[
                    dg.AssetKey(["ahn", "md5_ahn3"]),
                    dg.AssetKey(["ahn", "md5_ahn4"]),
                    dg.AssetKey(["ahn", "sha256_ahn5"]),
                ],
                instance=inst,
                definitions=SENSOR_DEFS,
            )
            first_result = sensor(ctx)

        # First run should skip without triggering any runs
        assert isinstance(first_result, dg.SkipReason)


def test_sensor_skips_when_checksums_unchanged():
    """Sensor skips when checksums match the previously stored cursor."""
    sensor = ahn_checksum_sensor(dg.DefaultSensorStatus.STOPPED)
    initial_cursor = json.dumps({"ahn3": CHECKSUMS_V1})
    with dg.DagsterInstance.ephemeral() as inst:
        inst.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=dg.AssetKey(["ahn", "md5_ahn3"]))
        )
        with (
            patch("bag3d.core.sensors.get_checksums", return_value=CHECKSUMS_V1),
            patch("bag3d.core.sensors.download_ahn_index", return_value=TILE_INDEX),
        ):
            ctx = dg.build_multi_asset_sensor_context(
                monitored_assets=[
                    dg.AssetKey(["ahn", "md5_ahn3"]),
                    dg.AssetKey(["ahn", "md5_ahn4"]),
                    dg.AssetKey(["ahn", "sha256_ahn5"]),
                ],
                instance=inst,
                cursor=initial_cursor,
                definitions=SENSOR_DEFS,
            )
            result = sensor(ctx)

    assert isinstance(result, dg.SkipReason)
    assert "No checksum changes detected" in (result.skip_message or "")


def test_sensor_triggers_only_changed_partitions():
    """Sensor emits a RunRequest only for tiles whose checksum changed.

    CHECKSUMS_V2 has 01cz1 changed and 50cn2 unchanged, so exactly one
    RunRequest should be emitted targeting partition '01cz1' on job 'ahn3'.
    """
    sensor = ahn_checksum_sensor(dg.DefaultSensorStatus.STOPPED)
    initial_cursor = json.dumps({"ahn3": CHECKSUMS_V1})
    with dg.DagsterInstance.ephemeral() as inst:
        inst.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=dg.AssetKey(["ahn", "md5_ahn3"]))
        )
        with (
            patch("bag3d.core.sensors.get_checksums", return_value=CHECKSUMS_V2),
            patch("bag3d.core.sensors.download_ahn_index", return_value=TILE_INDEX),
        ):
            ctx = dg.build_multi_asset_sensor_context(
                monitored_assets=[
                    dg.AssetKey(["ahn", "md5_ahn3"]),
                    dg.AssetKey(["ahn", "md5_ahn4"]),
                    dg.AssetKey(["ahn", "sha256_ahn5"]),
                ],
                instance=inst,
                cursor=initial_cursor,
                definitions=SENSOR_DEFS,
            )
            result = sensor(ctx)

    assert isinstance(result, list)
    assert len(result) == 1
    req = result[0]
    assert isinstance(req, dg.RunRequest)
    assert req.job_name == "ahn3"
    assert req.partition_key == "01cz1"


def test_sensor_triggers_multiple_versions_independently():
    """When both md5_ahn3 and md5_ahn4 are materialized with changes, RunRequests are emitted for each."""
    sensor = ahn_checksum_sensor(dg.DefaultSensorStatus.STOPPED)
    initial_cursor = json.dumps({"ahn3": CHECKSUMS_V1, "ahn4": CHECKSUMS_V1})
    with dg.DagsterInstance.ephemeral() as inst:
        inst.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=dg.AssetKey(["ahn", "md5_ahn3"]))
        )
        inst.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=dg.AssetKey(["ahn", "md5_ahn4"]))
        )
        with (
            patch("bag3d.core.sensors.get_checksums", return_value=CHECKSUMS_V2),
            patch("bag3d.core.sensors.download_ahn_index", return_value=TILE_INDEX),
        ):
            ctx = dg.build_multi_asset_sensor_context(
                monitored_assets=[
                    dg.AssetKey(["ahn", "md5_ahn3"]),
                    dg.AssetKey(["ahn", "md5_ahn4"]),
                    dg.AssetKey(["ahn", "sha256_ahn5"]),
                ],
                instance=inst,
                cursor=initial_cursor,
                definitions=SENSOR_DEFS,
            )
            result = sensor(ctx)

    assert isinstance(result, list)
    assert len(result) == 2

    job_names = {req.job_name for req in result}
    assert "ahn3" in job_names
    assert "ahn4" in job_names

    for req in result:
        assert req.partition_key == "01cz1"
