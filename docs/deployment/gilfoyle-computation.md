# Merged PR computation

`.github/workflows/gilfoyle-compute.yml` dispatches admin's Gilfoyle workflow when
a PR targeting `develop` closes as merged. It uses that PR's exact merge commit
SHA. Direct pushes do not trigger it. Orchestration runs on a GitHub-hosted runner;
admin executes through Kaiju and the existing Gilfoyle deployment account.

Configure the expiring `ADMIN_ACTIONS_DISPATCH_TOKEN` secret: a fine-grained PAT
scoped to `3DBAG/3dbag-admin` with Actions write. Publish admin's receiver and prove
a direct `deploy-and-run` before merging this workflow.

The API dispatch response identifies the exact admin run. The source Action links
it, polls every 30 seconds, and reports run/job/step transitions and final outcome.
Admin has a five-hour execution timeout; this observer has six hours. Admin's
concurrency allows the active run to finish and keeps only the newest pending
request. Superseded pending requests report cancellation. A remote failure or
cancellation fails observation; it never reports success.

Observation timeout or API loss reports an unresolved result with the admin link.
Remote computation may continue. Inspect active runner containers and Dagster
runs before dispatching another computation; ambiguous dispatches are not retried.
No preset, stages, notification or publication overrides are sent.
