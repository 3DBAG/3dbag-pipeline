
## Gilfoyle merged-PR computation

A PR merged into `develop` triggers `.github/workflows/gilfoyle-compute.yml`.
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
Compose overlay. b3d accepts fixture version `"2"` or numeric `2`, checks the AOI
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
