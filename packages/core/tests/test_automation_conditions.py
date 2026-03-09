"""Tests for automation conditions on core source and input asset chains.

Uses dagster.evaluate_automation_conditions which evaluates AutomationCondition
logic without executing asset functions (no resources or database needed).

Assets are loaded via asset_groups which apply the correct key_prefix:
- BAG:     key_prefix="bag"     → bag/extract_bag, bag/stage_bag_*, bag/bag_*actueelbestaand
- BGT:     key_prefix="bgt"     → bgt/extract_bgt, bgt/stage_bgt_pand, bgt/bgt_pandactueelbestaand
- TOP10NL: key_prefix="top10nl" → top10nl/extract_top10nl, top10nl/stage_top10nl_gebouw, top10nl/top10nl_gebouw
- Input:   key_prefix="input"   → input/intermediary/bag_kas_warenhuis, etc.

Three independent source chains triggered by cron on the 9th of each month:
  BAG:      0 0 9 * *  (midnight)
  BGT:      0 6 9 * *  (06:00)
  TOP10NL:  0 12 9 * * (noon)

Chain propagation tests use a pre-materialized baseline so that:
- eager() is quiet (nothing missing or stale)
- Re-materializing a root asset triggers only its direct downstream
- This isolates each layer of the chain
"""

import datetime

import dagster as dg
import pytest

from bag3d.core.asset_groups import bag_assets, bgt_assets, top10nl_assets, input_assets

AUTOMATED_ASSETS = [
    *bag_assets,
    *bgt_assets,
    *top10nl_assets,
    *input_assets,
]

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
}

DEFS = dg.Definitions(assets=AUTOMATED_ASSETS, resources=MOCK_RESOURCES)

# Time before any cron fires
BEFORE_CRON = datetime.datetime(2025, 1, 8, 23, 59, tzinfo=datetime.timezone.utc)
# Time after all three crons have fired (BAG: 00:00, BGT: 06:00, TOP10NL: 12:00 on 9th)
AFTER_CRON = datetime.datetime(2026, 2, 9, 12, 1, tzinfo=datetime.timezone.utc)

# Asset keys with the correct prefixes as applied by asset_groups
KEY_EXTRACT_BAG = dg.AssetKey(["bag", "extract_bag"])
KEY_EXTRACT_BGT = dg.AssetKey(["bgt", "extract_bgt"])
KEY_EXTRACT_TOP10NL = dg.AssetKey(["top10nl", "extract_top10nl"])

KEY_STAGE_BAG_WOONPLAATS = dg.AssetKey(["bag", "stage_bag_woonplaats"])
KEY_STAGE_BAG_VERBLIJFSOBJECT = dg.AssetKey(["bag", "stage_bag_verblijfsobject"])
KEY_STAGE_BAG_PAND = dg.AssetKey(["bag", "stage_bag_pand"])
KEY_STAGE_BAG_OPENBARERUIMTE = dg.AssetKey(["bag", "stage_bag_openbareruimte"])
KEY_STAGE_BAG_NUMMERAANDUIDING = dg.AssetKey(["bag", "stage_bag_nummeraanduiding"])

KEY_BAG_WOONPLAATSACTUEELBESTAAND = dg.AssetKey(
    ["bag", "bag_woonplaatsactueelbestaand"]
)
KEY_BAG_VERBLIJFSOBJECTACTUEELBESTAAND = dg.AssetKey(
    ["bag", "bag_verblijfsobjectactueelbestaand"]
)
KEY_BAG_PANDACTUEELBESTAAND = dg.AssetKey(["bag", "bag_pandactueelbestaand"])
KEY_BAG_OPENBARERUIMTEACTUEELBESTAAND = dg.AssetKey(
    ["bag", "bag_openbareruimteactueelbestaand"]
)
KEY_BAG_NUMMERAANDUIDINGACTUEELBESTAAND = dg.AssetKey(
    ["bag", "bag_nummeraanduidingactueelbestaand"]
)

KEY_STAGE_BGT_PAND = dg.AssetKey(["bgt", "stage_bgt_pand"])
KEY_BGT_PANDACTUEELBESTAAND = dg.AssetKey(["bgt", "bgt_pandactueelbestaand"])

KEY_STAGE_TOP10NL_GEBOUW = dg.AssetKey(["top10nl", "stage_top10nl_gebouw"])
KEY_TOP10NL_GEBOUW = dg.AssetKey(["top10nl", "top10nl_gebouw"])

KEY_BAG_KAS_WARENHUIS = dg.AssetKey(["input", "intermediary", "bag_kas_warenhuis"])
KEY_BAG_BAG_OVERLAP = dg.AssetKey(["input", "intermediary", "bag_bag_overlap"])
KEY_RECONSTRUCTION_INPUT = dg.AssetKey(["input", "reconstruction_input"])
KEY_TILES = dg.AssetKey(["input", "tiles"])
KEY_INDEX = dg.AssetKey(["input", "index"])

# All asset keys for pre-materialization
ALL_KEYS = [
    KEY_EXTRACT_BAG,
    KEY_EXTRACT_BGT,
    KEY_EXTRACT_TOP10NL,
    KEY_STAGE_BAG_WOONPLAATS,
    KEY_STAGE_BAG_VERBLIJFSOBJECT,
    KEY_STAGE_BAG_PAND,
    KEY_STAGE_BAG_OPENBARERUIMTE,
    KEY_STAGE_BAG_NUMMERAANDUIDING,
    KEY_BAG_WOONPLAATSACTUEELBESTAAND,
    KEY_BAG_VERBLIJFSOBJECTACTUEELBESTAAND,
    KEY_BAG_PANDACTUEELBESTAAND,
    KEY_BAG_OPENBARERUIMTEACTUEELBESTAAND,
    KEY_BAG_NUMMERAANDUIDINGACTUEELBESTAAND,
    KEY_STAGE_BGT_PAND,
    KEY_BGT_PANDACTUEELBESTAAND,
    KEY_STAGE_TOP10NL_GEBOUW,
    KEY_TOP10NL_GEBOUW,
    KEY_BAG_KAS_WARENHUIS,
    KEY_BAG_BAG_OVERLAP,
    KEY_RECONSTRUCTION_INPUT,
    KEY_TILES,
    KEY_INDEX,
]


def _requested_keys(result) -> set[dg.AssetKey]:
    """Return the set of AssetKeys marked as requested in this evaluation."""
    return {r.key for r in result.results if result.get_num_requested(r.key) > 0}


def _mat(instance, *keys: dg.AssetKey) -> None:
    """Report runless materializations for the given asset keys."""
    for key in keys:
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=key))


@pytest.fixture
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


@pytest.fixture
def instance_all_materialized(instance):
    """Instance with all 22 automated assets already materialized (clean baseline).

    Starting from a fully-up-to-date state means eager() is quiet until something
    is re-materialized upstream, isolating which downstream assets react.
    """
    _mat(instance, *ALL_KEYS)
    # Establish a cursor baseline before any cron fires
    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
    )
    assert _requested_keys(result) == set(), "Expected no requests in clean baseline"
    return instance, result.cursor


# ---------------------------------------------------------------------------
# Cron scheduling tests
# ---------------------------------------------------------------------------


def test_cron_roots_not_requested_before_tick(instance):
    """Root assets should not be requested before midnight on the 9th.

    on_cron only fires when a cron tick has passed between two evaluations. On the
    first evaluation before the tick no cron has fired, so the condition stays False.
    """
    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
    )
    requested = _requested_keys(result)
    assert KEY_EXTRACT_BAG not in requested
    assert KEY_EXTRACT_BGT not in requested
    assert KEY_EXTRACT_TOP10NL not in requested


def test_cron_roots_requested_after_tick(instance_all_materialized):
    """All three cron-triggered root assets are requested once their cron tick passes.

    Two evaluations are required: the first establishes the cursor (before the tick),
    the second detects the tick has passed.
    """
    instance, cursor = instance_all_materialized
    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=AFTER_CRON,
        cursor=cursor,
    )
    requested = _requested_keys(result)
    assert KEY_EXTRACT_BAG in requested
    assert KEY_EXTRACT_BGT in requested
    assert KEY_EXTRACT_TOP10NL in requested


# ---------------------------------------------------------------------------
# BAG chain propagation tests
# ---------------------------------------------------------------------------


def test_bag_stage_assets_requested_after_extract_bag(instance_all_materialized):
    """When extract_bag is re-materialized, all five stage_bag_* assets are requested."""
    instance, cursor = instance_all_materialized

    _mat(instance, KEY_EXTRACT_BAG)

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    requested = _requested_keys(result)
    assert KEY_STAGE_BAG_WOONPLAATS in requested
    assert KEY_STAGE_BAG_VERBLIJFSOBJECT in requested
    assert KEY_STAGE_BAG_PAND in requested
    assert KEY_STAGE_BAG_OPENBARERUIMTE in requested
    assert KEY_STAGE_BAG_NUMMERAANDUIDING in requested


def test_bag_load_assets_requested_after_stage_bag(instance_all_materialized):
    """When stage_bag_* assets are re-materialized, the bag_*actueelbestaand assets are requested."""
    instance, cursor = instance_all_materialized

    _mat(
        instance,
        KEY_STAGE_BAG_WOONPLAATS,
        KEY_STAGE_BAG_VERBLIJFSOBJECT,
        KEY_STAGE_BAG_PAND,
        KEY_STAGE_BAG_OPENBARERUIMTE,
        KEY_STAGE_BAG_NUMMERAANDUIDING,
    )

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    requested = _requested_keys(result)
    assert KEY_BAG_WOONPLAATSACTUEELBESTAAND in requested
    assert KEY_BAG_VERBLIJFSOBJECTACTUEELBESTAAND in requested
    assert KEY_BAG_PANDACTUEELBESTAAND in requested
    assert KEY_BAG_OPENBARERUIMTEACTUEELBESTAAND in requested
    assert KEY_BAG_NUMMERAANDUIDINGACTUEELBESTAAND in requested


# ---------------------------------------------------------------------------
# BGT chain propagation tests
# ---------------------------------------------------------------------------


def test_bgt_stage_requested_after_extract_bgt(instance_all_materialized):
    """When extract_bgt is re-materialized, stage_bgt_pand is requested."""
    instance, cursor = instance_all_materialized

    _mat(instance, KEY_EXTRACT_BGT)

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    assert KEY_STAGE_BGT_PAND in _requested_keys(result)


def test_bgt_load_requested_after_stage_bgt(instance_all_materialized):
    """When stage_bgt_pand is re-materialized, bgt_pandactueelbestaand is requested."""
    instance, cursor = instance_all_materialized

    _mat(instance, KEY_STAGE_BGT_PAND)

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    assert KEY_BGT_PANDACTUEELBESTAAND in _requested_keys(result)


# ---------------------------------------------------------------------------
# TOP10NL chain propagation tests
# ---------------------------------------------------------------------------


def test_top10nl_stage_requested_after_extract_top10nl(instance_all_materialized):
    """When extract_top10nl is re-materialized, stage_top10nl_gebouw is requested."""
    instance, cursor = instance_all_materialized

    _mat(instance, KEY_EXTRACT_TOP10NL)

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    assert KEY_STAGE_TOP10NL_GEBOUW in _requested_keys(result)


def test_top10nl_load_requested_after_stage_top10nl(instance_all_materialized):
    """When stage_top10nl_gebouw is re-materialized, top10nl_gebouw is requested."""
    instance, cursor = instance_all_materialized

    _mat(instance, KEY_STAGE_TOP10NL_GEBOUW)

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    assert KEY_TOP10NL_GEBOUW in _requested_keys(result)


# ---------------------------------------------------------------------------
# Input chain convergence tests
# ---------------------------------------------------------------------------


def test_intermediary_assets_requested_after_source_update(instance_all_materialized):
    """When bag_pandactueelbestaand updates, both intermediary assets are requested."""
    instance, cursor = instance_all_materialized

    _mat(instance, KEY_BAG_PANDACTUEELBESTAAND)

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    requested = _requested_keys(result)
    assert KEY_BAG_KAS_WARENHUIS in requested
    assert KEY_BAG_BAG_OVERLAP in requested


def test_reconstruction_input_requested_after_intermediary_update(
    instance_all_materialized,
):
    """When intermediary assets update, reconstruction_input is requested."""
    instance, cursor = instance_all_materialized

    _mat(instance, KEY_BAG_KAS_WARENHUIS, KEY_BAG_BAG_OVERLAP)

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    assert KEY_RECONSTRUCTION_INPUT in _requested_keys(result)


def test_tiles_and_index_requested_after_reconstruction_input_update(
    instance_all_materialized,
):
    """When reconstruction_input updates, tiles and index are both requested."""
    instance, cursor = instance_all_materialized

    _mat(instance, KEY_RECONSTRUCTION_INPUT)

    result = dg.evaluate_automation_conditions(
        defs=DEFS,
        instance=instance,
        evaluation_time=BEFORE_CRON,
        cursor=cursor,
    )
    requested = _requested_keys(result)
    assert KEY_TILES in requested
    assert KEY_INDEX in requested
