# Plan: Native Dagster orchestration for the full pipeline

## Goal

Run the complete 3DBAG pipeline using Dagster schedules, sensors, partitions, and run coordination, without relying on `3dbag-admin/deployment/3dbag-pipeline/runner.py`.

Preserve the existing `core`, `party_walls`, `floors_estimation`, and `export` code locations. A single Dagster job cannot directly span these locations, so the native implementation must coordinate jobs and asset materializations across the deployment.

## Implementation

### 1. Add a native pipeline kickoff

Add a core-location schedule or manually launchable kickoff job that starts:

- `source_input`;
- `ahn_tile_index`.

The kickoff must assign a shared pipeline-run identifier and a mode/input tag. In integration mode it must also identify the fixture manifest and selected partitions.

Move the runner’s concurrency-pool setup into deployment configuration or a Dagster startup/bootstrap step. Pool limits must not depend on a GraphQL mutation performed by the external runner.

### 2. Coordinate core stages with sensors

Add core-location run-status sensors for the local sequence:

1. after `ahn_tile_index` succeeds, launch the AHN 3/4/5 partitioned jobs for the selected partition set;
2. after all required AHN partition runs succeed, launch `ahn_metadata_index`;
3. after source input and AHN metadata are ready, register reconstruction partitions and launch `reconstruct`.

Each sensor must use a persisted cursor and deterministic run keys. It must not launch a downstream stage after only one partition succeeds.

### 3. Replace import-time reconstruction partitions

The current reconstruction partition definition queries the database during code-location import. This currently requires the runner to reload the code location after input preparation.

Replace this with a stable Dagster dynamic-partition definition:

- declare the dynamic partition definition at import time;
- after `input.tiles` is materialized, query the resulting tile IDs;
- add those IDs to the dynamic partition set;
- launch reconstruction runs for those keys;
- remove the dependency on code-location reloads for normal orchestration.

Fixture mode must derive its AHN and reconstruction partition selections from the snapshot manifest instead of from the user deployment’s hard-coded preset.

### 4. Add cross-location stage sensors

Add deployment-wide sensors in the downstream locations:

- `party_walls` sensor: monitor successful completion of the core reconstruction stage, then request `party_walls`;
- `floors_estimation` sensor: monitor successful `party_walls`, then request `floors_estimation`;
- `export` sensor: monitor successful `floors_estimation`, then request `export`.

Use native Dagster asset/run sensors with `monitor_all_code_locations` where appropriate. Each stage request must carry the shared pipeline-run identifier, release version, input mode, and stage name.

The completion barrier for partitioned stages must track the expected partition set and terminal status. A barrier must be satisfied only when every required partition succeeds; failures must stop downstream execution and remain visible in Dagster run history.

### 5. Move remaining runner responsibilities into Dagster

- Make reconstruction and export output preparation idempotent, or add dedicated native preparation assets/jobs for stage cleanup.
- Configure retry and partial-failure behavior on the Dagster jobs/sensors rather than in runner polling code.
- Use Dagster run status sensors and configured alerting for failure summaries and notifications.
- Use Dagster run monitoring, tags, event logs, and the UI instead of runner-side polling and hardware summaries.
- Keep the existing asset dependencies as the source of truth for data handoffs; do not introduce duplicate sequencing metadata in the deployment manifest unless required for partition-barrier state.

## Interfaces and state

Use the following run-tag contract:

```text
3dbag/pipeline-run   <unique pipeline execution id>
3dbag/stage          <stage name>
3dbag/input-mode     <production|testing|integration_data>
3dbag/expected-keys  <serialized expected partition keys, where applicable>
```

Sensor cursors must record the stage-run identifier, expected partitions, observed successes/failures, and whether the downstream request has already been emitted. Re-evaluating a sensor after a daemon restart must be safe.

## Validation

- Validate every code location with `dg check defs`.
- Test dynamic reconstruction partition registration from a materialized `input.tiles` asset.
- Test AHN, reconstruction, and export completion barriers with zero, partial, successful, and failed partition sets.
- Test cross-location sensors and duplicate-run prevention after sensor restarts.
- Run the complete integration-data pipeline without `runner.py`, from kickoff through export.
- Verify that production mode still selects the full source datasets and that fixture mode performs no source downloads.
- Confirm concurrency limits, retries, cleanup, and failure notifications work when the runner container is absent.

## Assumptions

- The existing four code locations remain separate.
- Native Dagster sensors are acceptable as the cross-location orchestration mechanism.
- The integration-data snapshot remains a read-only external fixture.
- Existing asset keys and file/database stage handoffs remain compatible; changes are limited to partition registration, orchestration, execution state, and deployment configuration.
