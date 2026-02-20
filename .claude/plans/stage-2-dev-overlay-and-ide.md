# Stage 2: Dev Compose Overlay and IDE Integration

## Goal

Create a development-specific compose override that bind-mounts source code (eliminating even the sync step), exposes the Docker socket to pipeline containers (prerequisite for Stage 3), and documents a PyCharm setup that works for both code analysis and in-container test execution.

---

## 2.1 Create `docker/compose.dev.yaml`

A development overlay that layers on top of the base `docker/compose.yaml`. The key additions:

1. **Bind-mount source code** — replaces the baked-in `/opt/3dbag-pipeline/packages/` with the host's live source. Edits are visible instantly, no sync or rebuild needed.
2. **Mount Docker socket** on pipeline containers — needed for Stage 3 (running tools in standalone containers from within asset code).
3. **Optional: profiles for selective startup** — let developers start only the services they need.

### File: `docker/compose.dev.yaml`

```yaml
services:
  bag3d-core:
    volumes:
      - bag3d-data-pipeline:/data/volume
      - bag3d-dagster-home:/opt/dagster/dagster_home
      - ~/.ssh:/root/.ssh:ro
      # Bind-mount source for live editing
      - ../packages/common/src:/opt/3dbag-pipeline/packages/common/src
      - ../packages/core/src:/opt/3dbag-pipeline/packages/core/src
      # Docker socket for running tools in standalone containers (Stage 3)
      - /var/run/docker.sock:/var/run/docker.sock

  bag3d-floors-estimation:
    volumes:
      - bag3d-data-pipeline:/data/volume
      - bag3d-dagster-home:/opt/dagster/dagster_home
      - ../packages/common/src:/opt/3dbag-pipeline/packages/common/src
      - ../packages/floors_estimation/src:/opt/3dbag-pipeline/packages/floors_estimation/src
      - /var/run/docker.sock:/var/run/docker.sock

  bag3d-party-walls:
    volumes:
      - bag3d-data-pipeline:/data/volume
      - bag3d-dagster-home:/opt/dagster/dagster_home
      - ../packages/common/src:/opt/3dbag-pipeline/packages/common/src
      - ../packages/party_walls/src:/opt/3dbag-pipeline/packages/party_walls/src
      - /var/run/docker.sock:/var/run/docker.sock
```

### Important: volumes key behavior

The `volumes` key in the overlay **replaces** the base service's volumes list (compose merge behavior for lists). So we must repeat the base volumes (`bag3d-data-pipeline`, `bag3d-dagster-home`, `~/.ssh`) in addition to the new bind-mounts. Alternatively, we can use the `!override` YAML tag (already used in compose-prod.yaml) for explicit intent.

---

## 2.2 Update makefile targets

### New targets in `makefile`

```makefile
# Development with bind-mounted source (instant code changes, no sync/rebuild)
docker_dev:
	BAG3D_DOCKER_IMAGE_TAG=$(BAG3D_DOCKER_IMAGE_TAG) docker compose -p $(COMPOSE_PROJECT_NAME) \
		-f docker/compose.yaml -f docker/compose.dev.yaml up -d

# Development with file watching (sync+restart on changes)
docker_watch:
	BAG3D_DOCKER_IMAGE_TAG=$(BAG3D_DOCKER_IMAGE_TAG) docker compose -p $(COMPOSE_PROJECT_NAME) \
		-f docker/compose.yaml watch
```

The existing `docker_watch` target stays as-is (uses Stage 1 sync+restart). The new `docker_dev` target uses bind-mounts for an even faster workflow — edit files on the host, they're immediately visible in the container (though the gRPC code server may need a manual reload via the Dagster UI "Reload" button, or a container restart).

### Update test targets to support the dev overlay

The `make test` targets use `docker compose exec` which works the same regardless of whether bind-mounts or baked-in code is used. No changes needed for test targets.

---

## 2.3 Code server auto-reload with bind mounts

With bind-mounted source, Dagster's gRPC code server doesn't automatically detect file changes (unlike `docker compose watch` which triggers a restart). Two options:

**Option A: Manual reload** — Click "Reload" in the Dagster UI after editing code. Simple but requires a manual step.

**Option B: watchfiles-based auto-restart** — Override the CMD in the dev overlay to wrap the dagster code-server with a file watcher:

```yaml
  bag3d-core:
    # ... volumes ...
    command: >
      watchmedo auto-restart --directory=/opt/3dbag-pipeline/packages/core/src
      --directory=/opt/3dbag-pipeline/packages/common/src --recursive --pattern="*.py"
      -- dagster code-server start -h 0.0.0.0 -p 4000 -m bag3d.core.code_location
```

This requires `watchdog` to be installed in the container (`pip install watchdog`). It would need to be added to the tools requirements or installed in the dev overlay via entrypoint.

**Recommendation:** Start with Option A (manual reload). It's simple and doesn't add dependencies. The `docker compose watch` with `sync+restart` from Stage 1 already handles auto-restart for the watch workflow.

---

## 2.4 PyCharm setup documentation

### The two-environment problem

- **Code analysis** (autocompletion, type checking, go-to-definition): needs a Python interpreter with all packages installed
- **Test execution / debugging**: needs the Docker environment (database, tools, env vars)

### Recommended setup

#### For code analysis: local venv

1. Run `make local_venv` to create per-package venvs
2. In PyCharm, go to `Settings > Project > Python Interpreter`
3. Add the venv for the package you're working on:
   - For core: `packages/core/.venv/bin/python`
   - For common: use core's venv (common is installed as editable dependency)
4. Mark source roots: `packages/core/src`, `packages/common/src`

This gives full autocompletion and type checking for all Python code. External tool binaries (roofer, tyler, etc.) aren't needed for code analysis.

#### For test execution: Docker Compose interpreter

1. In PyCharm, go to `Settings > Project > Python Interpreter > Add Interpreter > Docker Compose`
2. Configuration file: `docker/compose.yaml` (optionally add `docker/compose.dev.yaml`)
3. Service: `bag3d-core` (or the package you're working on)
4. Python path: `/opt/3dbag-pipeline/venv/bin/python`
5. Set path mappings: `<project-root>/packages → /opt/3dbag-pipeline/packages`

With the dev overlay (bind-mounted source), PyCharm executes tests inside the container but uses your local source files — so breakpoints and debugging work correctly.

#### Alternative: single interpreter with Remote Interpreter plugin

PyCharm Professional's "Docker Compose" interpreter can be set as the project default. It runs code analysis by indexing the remote interpreter's packages. This is slower than a local venv but avoids maintaining two environments.

### Where to document

Add a `docs/development/pycharm-setup.md` or add a section to the existing development documentation. Also add a brief note in `CLAUDE.md` under "Development Commands".

---

## 2.5 Selective service startup with profiles

For developers working on a single package, starting all three pipeline containers is wasteful. Add profiles to the compose overlay:

```yaml
services:
  bag3d-floors-estimation:
    profiles:
      - floors
      - all

  bag3d-party-walls:
    profiles:
      - party-walls
      - all
```

Then:
- `docker compose ... up -d` starts only core + databases + dagster (no floors/party-walls)
- `docker compose ... --profile floors up -d` also starts floors_estimation
- `docker compose ... --profile all up -d` starts everything

This is optional and can be added later if the full startup time is bothersome.

---

## Verification

1. Run `make docker_dev` — verify containers start with bind-mounted source
2. Edit a Python file on the host → verify the change is visible inside the container (`docker compose exec bag3d-core cat /opt/3dbag-pipeline/packages/core/src/bag3d/core/...`)
3. Click "Reload" in Dagster UI → verify the code location reloads with the change
4. Run `make test` → verify tests pass with bind-mounted source
5. Configure PyCharm Docker Compose interpreter → run a single test with a breakpoint → verify debugging works
