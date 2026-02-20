# Stage 1: Quick Wins — Faster Watch, Leaner Builds

## Goal

Eliminate full image rebuilds on every code change during `docker compose watch`, reduce build context size, harden the uv dependency caching in Dockerfiles, and fix misleading comments. Expected impact: code change → running code server in **seconds** instead of **minutes**.

---

## 1.1 Switch watch rules from `rebuild` to `sync+restart`

### Background

The compose.yaml comment (lines 97-102) claims `DockerRunLauncher` is in use, but `docker/dagster/dagster.yaml` (line 7) actually configures `DefaultRunLauncher`. With `DefaultRunLauncher` + gRPC code servers, runs execute as subprocesses spawned by the gRPC code server process — not as new Docker containers from the image. Therefore `action: sync+restart` is sufficient: sync the changed files into the running container, restart the code server process, and new runs use the updated code.

### Changes to `docker/compose.yaml`

For each of `bag3d-core`, `bag3d-floors-estimation`, `bag3d-party-walls`, replace the `develop.watch` block. Use `sync+restart` for source directories, and `rebuild` only for dependency metadata files (pyproject.toml, uv.lock) that require a `uv sync` inside the container.

**bag3d-core** (replace lines 103-116):
```yaml
    develop:
      watch:
        - action: sync+restart
          path: ../packages/common/src
          target: /opt/3dbag-pipeline/packages/common/src
        - action: sync+restart
          path: ../packages/core/src
          target: /opt/3dbag-pipeline/packages/core/src
        - action: rebuild
          path: ../packages/common/pyproject.toml
          target: /opt/3dbag-pipeline/packages/common/pyproject.toml
        - action: rebuild
          path: ../packages/core/pyproject.toml
          target: /opt/3dbag-pipeline/packages/core/pyproject.toml
        - action: rebuild
          path: ../packages/core/uv.lock
          target: /opt/3dbag-pipeline/packages/core/uv.lock
```

**bag3d-floors-estimation** (replace lines 147-160):
```yaml
    develop:
      watch:
        - action: sync+restart
          path: ../packages/common/src
          target: /opt/3dbag-pipeline/packages/common/src
        - action: sync+restart
          path: ../packages/floors_estimation/src
          target: /opt/3dbag-pipeline/packages/floors_estimation/src
        - action: rebuild
          path: ../packages/common/pyproject.toml
          target: /opt/3dbag-pipeline/packages/common/pyproject.toml
        - action: rebuild
          path: ../packages/floors_estimation/pyproject.toml
          target: /opt/3dbag-pipeline/packages/floors_estimation/pyproject.toml
        - action: rebuild
          path: ../packages/floors_estimation/uv.lock
          target: /opt/3dbag-pipeline/packages/floors_estimation/uv.lock
```

**bag3d-party-walls** (replace lines 191-204):
```yaml
    develop:
      watch:
        - action: sync+restart
          path: ../packages/common/src
          target: /opt/3dbag-pipeline/packages/common/src
        - action: sync+restart
          path: ../packages/party_walls/src
          target: /opt/3dbag-pipeline/packages/party_walls/src
        - action: rebuild
          path: ../packages/common/pyproject.toml
          target: /opt/3dbag-pipeline/packages/common/pyproject.toml
        - action: rebuild
          path: ../packages/party_walls/pyproject.toml
          target: /opt/3dbag-pipeline/packages/party_walls/pyproject.toml
        - action: rebuild
          path: ../packages/party_walls/uv.lock
          target: /opt/3dbag-pipeline/packages/party_walls/uv.lock
```

### Why only `src/` directories?

Watching `../packages/core` (the whole package dir) would also trigger on test file changes, README changes, etc. Watching `src/` only triggers on actual source code changes that the code server needs.

---

## 1.2 Fix the outdated DockerRunLauncher comment

### Change in `docker/compose.yaml`

Replace lines 97-102 (the comment block above `develop:` in `bag3d-core`) with:

```yaml
    # docker compose watch: sync+restart syncs changed source files into the
    # running container and restarts the gRPC code server. Since we use
    # DefaultRunLauncher, runs execute as subprocesses of the code server, so
    # they pick up the synced code. Dependency file changes (pyproject.toml,
    # uv.lock) still trigger a full rebuild since they require uv sync.
```

---

## 1.3 Targeted COPY in Dockerfiles

### Current problem

`COPY . $BAG3D_PIPELINE_LOCATION` copies the entire repo (filtered by `.dockerignore`). A change to `packages/party_walls/` invalidates the `bag3d-core` image layer cache even though core doesn't use party_walls code.

### Changes

**`docker/pipeline/bag3d-core.dockerfile`** — replace line 28:
```dockerfile
# Before:
# COPY . $BAG3D_PIPELINE_LOCATION

# After:
COPY packages/common $BAG3D_PIPELINE_LOCATION/packages/common
COPY packages/core $BAG3D_PIPELINE_LOCATION/packages/core
```

**`docker/pipeline/bag3d-floors-estimation.dockerfile`** — replace line 28:
```dockerfile
COPY packages/common $BAG3D_PIPELINE_LOCATION/packages/common
COPY packages/floors_estimation $BAG3D_PIPELINE_LOCATION/packages/floors_estimation
```

**`docker/pipeline/bag3d-party-walls.dockerfile`** — replace line 41:
```dockerfile
COPY packages/common $BAG3D_PIPELINE_LOCATION/packages/common
COPY packages/party_walls $BAG3D_PIPELINE_LOCATION/packages/party_walls
```

### Caveat

The `uv sync` step after COPY uses the lock file at `$BAG3D_PIPELINE_LOCATION/packages/<pkg>/uv.lock`. Since we're copying the whole package directory, the `uv.lock` and `pyproject.toml` are included. The `bag3d-common` dependency is a `path = "../common"` reference in pyproject.toml, so `packages/common` must also be copied. This is already covered above.

---

## 1.4 Harden the uv dependency caching in Dockerfiles

### Analysis of the current two-step caching strategy

The Dockerfiles use a well-designed two-step `uv sync` pattern:

**Step 1 — dependency-only install (layer-cached by lock/pyproject content):**
```dockerfile
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./packages/core/uv.lock,target=.../uv.lock \
    --mount=type=bind,source=./packages/core/pyproject.toml,target=.../pyproject.toml \
    uv sync --frozen --all-extras --no-install-project --no-install-package bag3d-common \
    --project .../packages/core --python $VIRTUAL_ENV/bin/python
```

**Step 2 — full source copy (always invalidated by code changes):**
```dockerfile
COPY . $BAG3D_PIPELINE_LOCATION
```

**Step 3 — project install (cheap, just editable `.pth` links):**
```dockerfile
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-extras \
    --project .../packages/core --python $VIRTUAL_ENV/bin/python
```

This is the correct uv Docker caching pattern. How the caching layers interact:

- **Docker layer cache:** BuildKit hashes the bind-mounted file contents (`uv.lock`, `pyproject.toml`) as part of the layer cache key. If these files haven't changed, Step 1 is skipped entirely (layer reused). Step 3 always re-runs after COPY, but it's fast because all third-party deps are already installed.
- **BuildKit cache mount (`/root/.cache/uv`):** Persists across builds. Even when Step 1's layer IS invalidated (lock/pyproject changed), uv doesn't re-download packages that are already in this cache. All three pipeline images share this cache (same mount target), which is correct — they share most dependencies.
- **`--frozen`:** Prevents resolution. Reads the lockfile directly. Doesn't try to re-resolve sources or access the network.
- **`--no-install-package bag3d-common`:** Skips installing bag3d-common, but its transitive dependencies (docker, requests, fabric, psycopg, etc.) ARE installed because they're listed as independent packages in the lockfile.

### Issue: Missing bind-mount for `packages/common/pyproject.toml`

The lockfile for each package has `bag3d-common` declared as `source = { editable = "../common" }`. In Step 1, only the package's own `uv.lock` and `pyproject.toml` are bind-mounted — `packages/common/pyproject.toml` doesn't exist in the container at this point.

Currently this works because `--frozen` + `--no-install-package bag3d-common` means uv skips this package entirely without trying to read its source directory. However, this is **fragile**: a future uv version could validate editable source paths even when skipping install, or the behavior of `--frozen` with missing workspace members could change.

**Fix:** Add a bind-mount for common's `pyproject.toml` in Step 1 of all three Dockerfiles. This doesn't affect cache behavior (common's pyproject.toml changes very rarely, and only adds a second file to the cache key hash).

**`docker/pipeline/bag3d-core.dockerfile`** — replace lines 17-26:
```dockerfile
# Install only third-party dependencies (layer cached by lock/pyproject content)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./packages/core/uv.lock,target=$BAG3D_PIPELINE_LOCATION/packages/core/uv.lock \
    --mount=type=bind,source=./packages/core/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/core/pyproject.toml \
    --mount=type=bind,source=./packages/common/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/common/pyproject.toml \
    uv sync \
    --frozen \
    --all-extras \
    --no-install-project \
    --no-install-package bag3d-common \
    --project $BAG3D_PIPELINE_LOCATION/packages/core \
    --python $VIRTUAL_ENV/bin/python
```

**`docker/pipeline/bag3d-floors-estimation.dockerfile`** — same pattern:
```dockerfile
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./packages/floors_estimation/uv.lock,target=$BAG3D_PIPELINE_LOCATION/packages/floors_estimation/uv.lock \
    --mount=type=bind,source=./packages/floors_estimation/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/floors_estimation/pyproject.toml \
    --mount=type=bind,source=./packages/common/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/common/pyproject.toml \
    uv sync \
    --frozen \
    --all-extras \
    --no-install-project \
    --no-install-package bag3d-common \
    --project $BAG3D_PIPELINE_LOCATION/packages/floors_estimation \
    --python $VIRTUAL_ENV/bin/python
```

**`docker/pipeline/bag3d-party-walls.dockerfile`** — same pattern:
```dockerfile
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./packages/party_walls/uv.lock,target=$BAG3D_PIPELINE_LOCATION/packages/party_walls/uv.lock \
    --mount=type=bind,source=./packages/party_walls/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/party_walls/pyproject.toml \
    --mount=type=bind,source=./packages/common/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/common/pyproject.toml \
    uv sync \
    --frozen \
    --all-extras \
    --no-install-project \
    --no-install-package bag3d-common \
    --project $BAG3D_PIPELINE_LOCATION/packages/party_walls \
    --python $VIRTUAL_ENV/bin/python
```

### Note: unbounded uv cache growth

The `--mount=type=cache,target=/root/.cache/uv` BuildKit cache mount grows over time as package versions change. Old versions are never cleaned. This is one contributor to "excessive docker cache use."

To periodically clean it:
```bash
# Remove all BuildKit cache mounts
docker builder prune --filter type=exec.cachemount

# Or more aggressively, all BuildKit cache
docker builder prune --all
```

Consider adding a `make docker_prune_cache` target for this.

---

## 1.5 Expand `.dockerignore`

### Additions to `.dockerignore`

```
# Docker config (not needed inside pipeline images)
docker/

# Git and GitHub
.git/
.github/

# Root config files not needed in images
makefile
tools-build.sh
tools-test.sh
mkdocs.yml
CHANGELOG.md
LICENSE-*
README.md
CLAUDE.md
pyproject.toml
uv.lock

# Per-package local venvs
packages/*/.venv
```

### Note

The root `pyproject.toml` and `uv.lock` are for the dev workspace only — not used by the pipeline Dockerfiles (they each reference `packages/<pkg>/pyproject.toml`). The per-package `.venv` directories created by `make local_venv` can also be excluded.

### Interaction with targeted COPY (1.3)

With the targeted COPY approach from 1.3, the `.dockerignore` becomes less critical for excluding unrelated packages (since they're not copied anyway). However, it still reduces the **build context transfer size** — Docker sends the entire context to the daemon before builds begin, and `.dockerignore` controls what's included in that transfer. Excluding `.git/` alone can save hundreds of MB.

---

## Verification

1. Run `make docker_up` to build images with the updated Dockerfiles (one-time build)
2. Run `make docker_watch` to start compose watch
3. Edit a Python file in `packages/core/src/bag3d/core/` (e.g., add a log statement to an asset)
4. Observe the watch output: should show `Syncing...` and `Restarting...` (not `Rebuilding...`)
5. Check the Dagster UI at http://localhost:3003 — the code location should reload within seconds
6. Launch a test run in the UI and verify the log statement appears (confirming the synced code is used)
7. Edit `packages/core/pyproject.toml` (e.g., add a comment) — verify this triggers a full rebuild
8. Run `make test` to verify tests still pass with the targeted COPY approach

### Verify uv caching effectiveness

9. After the initial build, run `make docker_build` (no-cache build) and note the total build time
10. Change a single file in `packages/core/src/`, run `docker compose -p bag3d-dev -f docker/compose.yaml build bag3d-core` — Step 1 should be cached (instant), only COPY + Step 3 re-run
11. Change `packages/core/pyproject.toml`, run the same build — Step 1 should re-run (but fast due to uv download cache), then COPY + Step 3
12. Change a file in `packages/party_walls/src/`, build `bag3d-core` — with targeted COPY, the core image should be fully cached (no layers re-run)
