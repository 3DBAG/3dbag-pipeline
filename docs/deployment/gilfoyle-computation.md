# Reviewed PR merge gate

PRs targeting `develop` run unit tests and lint before review. After at least
one approving review, a maintainer clicks **Merge when ready** to enter GitHub's
merge queue. New commits dismiss stale approvals; review threads must be resolved.

`.github/workflows/integration.yml` reports a lightweight successful prerequisite
on the PR, then runs the actual computation on `merge_group: checks_requested`.
It sends the queue's immutable `head_sha` and `head_ref` to admin's `pipeline-dev.yml`
on `main`. Admin accepts the SHA only if it exactly matches the live
`refs/heads/gh-readonly-queue/develop/...` ref in the fixed pipeline repository.
Manual dispatches still require a SHA reachable from `develop`.

The required check is **Fixture-backed dev computation** (the job name, rather
than the workflow name **Test Integration**). The queue merges only after this
check, **Unit tests**, and **Lint** succeed on the queued commit. A failure removes
the PR from the queue. The queue builds and merges one entry at a time, with a
six-hour check timeout. Direct pushes and completed merges do not dispatch a
second computation. Orchestration runs on a GitHub-hosted runner; admin executes
through Kaiju and the existing Gilfoyle deployment account.

## Rollout

1. Merge the companion admin `pipeline-dev.yml` change into admin `main` first.
   It adds the optional `pipeline_ref` input and validates the exact queue SHA.
2. Merge these pipeline workflow changes into `develop` while the merge queue
   ruleset is disabled. All required workflows must support `merge_group`.
3. Enable [Develop reviewed integration merge queue](https://github.com/3DBAG/3dbag-pipeline/rules/26616856)
   in repository Settings → Rules → Rulesets. This ruleset is already created
   in disabled mode. Its definition is tracked in
   [`.github/develop-merge-gate.json`](../../.github/develop-merge-gate.json).
   The existing **Require pull request** ruleset remains in place for both
   `develop` and `master`; the new ruleset applies only to `develop`.
4. Approve and queue a PR, then follow **Test Integration** to its linked admin
   run. Verify the dispatched SHA matches the merge group, that computation
   succeeds before merge, and that a failed computation blocks merging.

The JSON defaults to disabled so importing it cannot strand existing PRs before
both workflow changes are deployed. After both changes are merged, activate the
existing ruleset through the UI or:

```sh
jq '.enforcement = "active"' .github/develop-merge-gate.json | \
  gh api --method PUT repos/3DBAG/3dbag-pipeline/rulesets/26616856 --input -
```

GitHub documents the merge queue and required `merge_group` triggers in
[Managing a merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue).

## Dispatch and observation

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
