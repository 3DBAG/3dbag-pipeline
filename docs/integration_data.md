# Gilfoyle merged-PR computation

A PR merged into `develop` triggers `.github/workflows/integration.yml`.
It dispatches the immutable merge SHA to admin's Gilfoyle workflow and observes
the exact admin run ID returned by GitHub. The source workflow succeeds only
when deployment, the complete computation through export, and scoped cleanup
all succeed. Follow the linked admin run for operation output.

Gilfoyle's workflow selects the existing snapshot explicitly:

```sh
export BAG3D_INTEGRATION_DATA_HOST_DIR=/data/3DBAG/pipeline/integration-data
b3d deploy dev "$pipeline_sha"
b3d run dev "$pipeline_sha"
```

The operation log records the executable version, full pipeline SHA, fixture
path and manifest SHA-256. The dev checkout must include the integration
Compose overlay. b3d accepts fixture versions `"2"`/`2` and `"3"`/`3`, checks the AOI
and required AHN partitions, and loads manifest-derived runner overrides after
the preset. The snapshot mounts read-only on core, Dagster webserver and daemon.
An empty AHN6 selection stays empty. Persistent presets and the snapshot are
preserved. Staging and production do not automatically select this overlay.

Acceptance uses the normal clean-run defaults. Failed deployments remain for
diagnosis; successful operations remove only their recorded deployment resources
and retain operation logs. There are no automatic computation retries. After
lost SSH, cancellation or timeout, inspect runner containers and Dagster activity
before retrying: an observer ending does not establish that computation stopped.
See the [admin runbook](https://github.com/3DBAG/3dbag-admin/blob/main/sysadmin/docs/gilfoyle-feature-deployment.md)
for locking, cleanup and recovery.

The snapshot directory may retain files from earlier extracts. Adapters verify
every file declared by the manifest and copy only those declared files into
the writable BAG input directory. Undeclared extras are left untouched and are
never consumed. Missing files, size mismatches and checksum mismatches still fail.

`extract_bag` publishes each download into a unique dated directory under
`lvbag-extract`, after extraction and metadata validation succeed. Snapshots retain
their layer ZIPs. Parallel BAG loading expands each layer in a temporary sibling
directory and removes that working directory on success or failure.

When building a fixture, `integration_bag` uses the `shortdate` returned by
`extract_bag` to select layer ZIPs and removes dated leftovers from its fixture
output after successful filtering. It leaves the source snapshot untouched.
Existing expanded-only extracts must be refreshed by rematerializing `extract_bag`
before building fixtures. Older snapshots and legacy files are retained; this
change does not introduce a retention policy.

Fixture adapters retain the ordinary source op names as well as their asset
keys, so the same runner configuration remains valid in fixture mode.

GitHub can return `404` for job details immediately after accepting a dispatch.
The observer continues polling the exact returned admin run ID when those details
are unavailable. The run's terminal conclusion still determines the source result;
this never submits another computation. Other observation errors retain the
unresolved-result reporting and inspection requirement.

## Generating native integration snapshots

Launch the `integration_data` job manually in Dagster or enable its monthly
schedule. The job defaults supply `AOI_WKT` to both `extract_bgt` and
`extract_top10nl`; manual job launches no longer require a separate AOI override.
Individual asset materializations outside this job still require explicit
extraction configuration. The supported snapshot AOI remains the fixed `AOI_WKT`.

Version 3 snapshots copy the PDOK BGT and TOP10NL ZIPs unchanged. PDOK selects
whole objects using the AOI; no local clipping or GML rewriting occurs. Building
footprints can extend outside the AOI plus 10 m. Validation checks native
geometry and AOI overlap using the production XSDs, while BAG, CBS and point
clouds retain the existing containment checks. TOP10NL point buildings remain
supported. Version 2 snapshots retain their original validation contract.

Upgrade pipeline, ctl and admin consumers before switching to a version 3
snapshot. Regenerate the extracts with the job's AOI defaults and rebuild the
manifest; existing nationwide archives and rewritten fixtures must not be
relabelled as version 3. Validate the replacement snapshot in an isolated
deployment before switching consumers, retaining the previous snapshot for
rollback. Failed GDAL imports now fail the staging asset instead of returning
success with missing metadata.

The native GML/PostGIS regression test requires `BAG3D_TEST_GDAL_BIN` (the
directory containing `ogrinfo`, `ogr2ogr`, and `sozip`) and
`BAG3D_TEST_POSTGRES_DSN` pointing to a disposable PostGIS database. It replaces
staging/output tables in that database. Run it with:

```sh
packages/core/.venv/bin/python -m pytest packages/core/tests/test_native_gml_postgis.py
```
