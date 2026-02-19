# ADR 0002: Bind Mounts over Docker Watch for Development

**Status:** Accepted
**Date:** 2026-02-19

## Context

The pipeline runs in Docker containers during development. Developers need changes to Python
source files to be reflected in the running containers without a full image rebuild. Two
mechanisms are available:

**Docker watch** (`docker compose watch`) — monitors the host filesystem and, on change, copies
modified files into the running container (`sync`) then restarts the gRPC code server
(`sync+restart`). Dependency file changes (`pyproject.toml`, `uv.lock`) trigger a full image
rebuild. Configured via `develop.watch` blocks in `compose.yaml`.

**Bind mounts** — the host source directory is mounted directly into the container as a volume
overlay. There is no copy step; the container sees the host filesystem in real time. The gRPC
code server must be reloaded manually via the Dagster UI "Reload" button after changes.

Both mechanisms were present simultaneously: `develop.watch` blocks in `compose.yaml` (activated
via `make docker_watch`) and a `compose.dev.yaml` overlay with bind mounts (activated via
`make docker_dev`).

Stage 3 of the pipeline refactoring introduces execution of external tools (roofer, tyler, GDAL,
etc.) in standalone Docker containers launched from within asset code via the Docker API. This
creates a new constraint on the development workflow.

## Decision

Use bind mounts (`make docker_dev`) as the primary development workflow. Retain `docker watch`
(`make docker_watch`) as an opt-in alternative. Do not remove either.

## Rationale

### Bind mounts are strictly faster

Docker watch has two phases: file detection, then a copy-and-restart cycle. The restart takes
2–5 seconds and the gRPC code server is unavailable during that time. With bind mounts, the
container sees the change at the filesystem level the moment the file is saved — there is no
intermediate copy step and no restart.

### Docker watch restarts are incompatible with running tool containers

Stage 3 asset code spawns standalone Docker containers (via `docker.from_env()`) to execute
tools such as roofer, tyler, and GDAL. These tool containers run as sub-tasks of the
pipeline container's gRPC code server process.

`docker watch` with `sync+restart` sends `SIGTERM` to the code server and starts a new process.
Any tool container that was launched by the previous code server process becomes an orphan: it
continues running but is no longer reachable by the pipeline. The asset execution that spawned it
is terminated, the output files it produces are left in an indeterminate state, and the orphaned
container must be cleaned up manually.

This is not a theoretical concern. Developers editing configuration files, flowcharts, or utility
modules while a reconstruction or tiling job is running would trigger an unintended restart. The
consequence — a partially-complete, unrecoverable asset materialisation — is worse than having
no auto-reload at all.

Bind mounts do not restart the code server. Tool containers continue running, their stdout/stderr
stream back to the Dagster logger, and the materialisation completes normally. A developer can
save files freely during a run; the changes take effect at the next Dagster code server reload
(manual) or the next run.

### Bind mounts are required for IDE debugging

PyCharm's Docker Compose interpreter attaches a debugger to a running container process. If that
process is restarted by `docker watch`, the debug session is terminated. Bind mounts keep the
container process stable and allow breakpoints to survive across file edits.

### When docker watch is still useful

Docker watch is the better choice when:

1. **No tool containers will be launched** — working on assets that call only Python code or
   database queries, with no external tool invocations.
2. **Auto-reload is preferred over manual reload** — iterating rapidly on asset logic and
   wanting the code server to pick up changes automatically rather than clicking "Reload" in the
   Dagster UI.
3. **Not using PyCharm debugging** — where the restart cost is acceptable.

These cases are common enough that removing `docker watch` would be a regression for some
development workflows.

### Dependency changes require a rebuild regardless

Both mechanisms require a manual `make docker_build` when `pyproject.toml` or `uv.lock` changes,
because `uv sync` must run to install new packages. Docker watch's `action: rebuild` automates
the trigger but the rebuild itself still takes minutes. This is not a differentiating factor.

## Consequences

- `make docker_dev` is the recommended development workflow, documented as such.
- `make docker_watch` remains available for workflows that do not involve tool containers or
  IDE debugging.
- After editing Python files with bind mounts, developers must click "Reload" in the Dagster UI
  (or restart the container) to pick up changes in the gRPC code server. This is documented in
  `docs/development/pycharm-setup.md`.
- Tool containers launched during a `make docker_dev` session are labelled with
  `bag3d.managed-by=3dbag-pipeline` and can be identified via
  `docker ps --filter label=bag3d.managed-by` if they need to be inspected or cleaned up after
  a crash.

## Related Files

| File                                                        | Role                                                                              |
|-------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `docker/compose.yaml`                                       | `develop.watch` blocks (docker watch); Docker socket mount on pipeline containers |
| `docker/compose.dev.yaml`                                   | Bind-mount overlay for source directories                                         |
| `makefile`                                                  | `docker_dev` and `docker_watch` targets                                           |
| `packages/common/src/bag3d/common/resources/executables.py` | `CommandRunner._run_docker()` — tool container lifecycle                          |
| `docs/development/pycharm-setup.md`                         | IDE setup and reload workflow                                                     |
