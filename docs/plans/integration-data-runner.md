# Plan: Run the canonical pipeline with integration data

## Goal

Allow the existing `3dbag-admin/deployment/3dbag-pipeline/runner.py` workflow to execute the canonical pipeline against the generated integration-data snapshot, with no changes to `runner.py` itself or only small deployment-wrapper changes.

The snapshot is treated as an external, read-only fixture. The `integration_data` job remains responsible for refreshing the fixture; it is not part of the canonical end-to-end run.

## Implementation

### 1. Enable the existing fixture mode

Use the existing fixture asset replacement in `packages/core/src/bag3d/core/asset_groups.py` and the adapters in `packages/core/src/bag3d/core/assets/fixture_adapters.py`.

The user deployment must set the following variables in every process that loads or executes core definitions:

```text
BAG3D_INPUT_MODE=integration_data
BAG3D_INTEGRATION_DATA_DIR=/data/volume/integration-data
```

Apply the existing `docker/compose.integration-data.yaml` overlay to both deployment startup and the `dagster-runner` Compose command. Pass the host snapshot through:

```text
BAG3D_INTEGRATION_DATA_HOST_DIR=/home/balazs/.config/JetBrains/PyCharm2026.2/docker/3dbag-pipeline-core-2026_07_29/data/volume/integration-data
```

The overlay must mount this directory read-only at `/data/volume/integration-data` in `bag3d-core`, `dagster-daemon`, and `dagster-webserver`.

### 2. Override the runner’s development preset

The current user preset targets a different AOI and AHN tile. Add an integration-specific environment file that is loaded after the normal `preset.env`:

```text
DATA_DEVELOP_AOI="POLYGON ((121967 485750, 123354 485750, 123354 486550, 121967 486550, 121967 485750))"
DATA_DEVELOP_AHN_TILES="25gn1"
```

Do not set `DATA_DEVELOP_AHN6_TILES` for the current snapshot because its manifest contains no AHN6 partitions. The runner will consequently skip the AHN6 stage in testing mode.

The environment file can be generated from `integration-data/manifest.json` so that future snapshots do not require hard-coded AOI or partition values.

### 3. Update the user deployment wrapper

Modify the user deployment files in `3dbag-admin/deployment/3dbag-pipeline/user/`:

- include `compose.integration-data.yaml` in the `deploy` and `test` Compose file lists;
- provide `BAG3D_INTEGRATION_DATA_HOST_DIR` as a configurable deployment variable;
- mount and pass the generated integration environment file to `runner.py` after `preset.env`;
- keep the existing `--mode testing` invocation and stage ordering.

No `runner.py` changes are required. Its existing testing configuration still supplies geofilters and AHN job configuration, while the core code location selects the fixture-backed assets through `BAG3D_INPUT_MODE`.

## Validation

- Render the final Compose configuration and verify that all three required services receive the fixture variables and read-only mount.
- Confirm the runner sees the snapshot AOI and `25gn1`, rather than the default user preset.
- Run `source_input` and verify that BAG, BGT, TOP10NL, CBS, and AHN inputs come from the snapshot without downloading source data.
- Run the complete fixture-backed pipeline through export.
- Verify that default/production input mode remains unchanged when the overlay is omitted.
- Add deployment-wrapper tests for overlay selection, environment-file ordering, and snapshot-path validation.

## Assumptions

- The snapshot is available on the deployment host at the supplied path and has a valid fixture-version-2 manifest.
- The snapshot is not copied into Git or modified by pipeline runs.
- The four existing Dagster code locations and the external runner remain unchanged for this plan.
