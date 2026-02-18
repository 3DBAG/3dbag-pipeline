# ADR 0001: AHN Checksum Monitoring — `multi_asset_sensor` over Declarative Automation

**Status:** Accepted
**Date:** 2026-02-18

## Context

AHN (Actueel Hoogtebestand Nederland) height-model tiles are periodically updated by PDOK. Each
tile is an independent partitioned asset in the pipeline. When PDOK releases updated tiles, only
the changed tiles should be re-processed — re-running all partitions would be wasteful and would
obscure which data actually changed.

To detect updates, PDOK publishes a checksum file alongside each release. The pipeline's
`ahn_checksum_sensor` (implemented in `packages/core/src/bag3d/core/sensors.py`) reads that
checksum file, compares it against a stored baseline in the sensor cursor, and emits a
`RunRequest` with the appropriate `partition_key` for each tile whose checksum changed.

When this sensor was implemented, Dagster emitted the following deprecation warning:

> `MultiAssetSensorDefinition` is superseded and its usage is discouraged. For most use cases,
> Declarative Automation should be used instead of `multi_asset_sensors`… In cases where side
> effects are required, or a specific job must be targeted for execution, `multi_asset_sensors`
> may be used.

This ADR documents why the `multi_asset_sensor` approach is correct for this use case and why
Declarative Automation (DA) cannot replace it.

## Decision

Keep the `multi_asset_sensor`-based `ahn_checksum_sensor` for checksum-driven selective
partition triggering. Do not replace it with Declarative Automation conditions.

## Rationale

There are three distinct capabilities the sensor requires that DA cannot provide:

### 1. External side effects

Declarative Automation operates exclusively on Dagster's internal asset graph state. Its
`AutomationCondition` primitives (`on_cron()`, `eager()`, `any_deps_updated()`, etc.) have no
mechanism to reach outside Dagster — they cannot make HTTP requests, read files from a remote
server, or otherwise inspect external systems.

The checksum sensor must download a file from a PDOK URL on every evaluation tick. This is an
external side effect that cannot be expressed as an `AutomationCondition`.

### 2. Stateful diff across evaluations

Even if DA could observe the checksum file, it has no concept of a persistent cursor. The sensor
stores the previous checksum snapshot in `context.cursor` and computes a diff on each run. DA
has no equivalent mechanism for persisting arbitrary state between evaluations and basing
triggering decisions on the delta.

### 3. Selective partition triggering

The most critical limitation: DA triggers asset materialization at the level of an asset
partition in response to asset graph events. There is no `AutomationCondition` that says
"trigger partition P only if an externally-computed condition holds for P specifically".

The sensor translates the checksum diff into individual `RunRequest(job_name="ahn3",
partition_key=tile_id)` calls — one per changed tile. This is the only Dagster mechanism for
dynamic, externally-driven, per-partition triggering.

## Effects on Partitioned Assets

If DA's `eager()` or `any_deps_updated()` were applied to a partitioned downstream asset, any
update to the upstream unpartitioned checksum asset would trigger **all** partitions — not just
the ones that changed. DA has no per-partition condition support for externally-derived
predicates.

The sensor avoids this by:
1. Evaluating which tile checksums changed.
2. Yielding a `RunRequest` only for those partition keys.
3. Leaving unchanged partitions untouched.

This is the exact carve-out described in Dagster's own deprecation notice: *"In cases where side
effects are required, or a specific job must be targeted for execution, `multi_asset_sensors` may
be used."* All three criteria apply here.

## Consequences

- **Deprecation warning** — Dagster will continue to emit a `MultiAssetSensorDefinition is
  superseded` warning in logs and test output for as long as the sensor exists. This is expected
  and acceptable given the rationale above.
- **Future revisitation** — If a future Dagster release introduces DA support for external data
  integration (e.g., an `on_external_condition()` primitive) or per-partition condition
  evaluation, this decision should be re-evaluated.
- **Test coverage** — `packages/core/tests/test_ahn_automation.py` covers the sensor's
  evaluation logic. Tests for the `AutomationConditionSensorDefinition` wrappers on related
  assets are included in the same file for completeness.

## Related Files

| File | Role |
|------|------|
| `packages/core/src/bag3d/core/sensors.py` | `ahn_checksum_sensor` implementation |
| `packages/core/tests/test_ahn_automation.py` | Sensor and automation condition tests |
