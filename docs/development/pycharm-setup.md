# PyCharm Setup

PyCharm has two distinct needs when working with this project:

- **Code analysis** (autocompletion, type checking, go-to-definition): needs a Python interpreter with all packages installed
- **Test execution / debugging**: needs the Docker environment (database, tools, environment variables)

The recommended approach is to configure a local venv for code analysis and a Docker Compose interpreter for running and debugging tests.

---

## Code analysis: local venv

1. Run `make local_venv` to create per-package virtual environments.
2. In PyCharm, go to **Settings → Project → Python Interpreter → Add Interpreter → Add Local Interpreter**.
3. Select **Existing** and point it to the venv for the package you're working on:
    - For `core` / `common`: `packages/core/.venv/bin/python`
    - For `floors_estimation`: `packages/floors_estimation/.venv/bin/python`
    - For `party_walls`: `packages/party_walls/.venv/bin/python`
4. Mark source roots so PyCharm resolves cross-package imports correctly:
    - Right-click `packages/core/src` → **Mark Directory as → Sources Root**
    - Right-click `packages/common/src` → **Mark Directory as → Sources Root**

This gives full autocompletion and type checking. External tool binaries (roofer, tyler, etc.) are not needed for code analysis.

---

## Test execution and debugging: Docker Compose interpreter

### Prerequisites

Start the containers with bind-mounted source so that breakpoints in your local files map directly to the running code:

```bash
make docker_dev
```

The `docker_dev` target starts all services with `docker/compose.dev.yaml` layered on top of the base compose file. The pipeline containers bind-mount the host source directories, so edits are
immediately visible inside the containers.

For the default local development stack, run `make docker_dev`.

### Configure the Docker Compose interpreter in PyCharm

1. Go to **Settings → Project → Python Interpreter → Add Interpreter → On Docker Compose**.
2. Set **Configuration files**:
    - `docker/compose.yaml`
    - `docker/compose.dev.yaml` (add with the `+` button)
3. Set **Service**: `bag3d-core` (or the package you're testing).
4. Set **Python interpreter path**: `/opt/3dbag-pipeline/venv/bin/python`
5. Under **Path mappings**, add:

| Local path                | Remote path                    |
|---------------------------|--------------------------------|
| `<project-root>/packages` | `/opt/3dbag-pipeline/packages` |

6. Click **OK** and wait for PyCharm to index the remote interpreter.

### Running tests with debugging

Once the Docker Compose interpreter is configured and the containers are running with bind-mounted source (`make docker_dev`):

- Right-click any test file or function and choose **Debug** — breakpoints in your local source files will be hit inside the container.
- The test environment (PostgreSQL, environment variables from `docker/.env`) is available automatically because the test runs inside the container.

---

## After editing Python files

With bind-mounted source (`make docker_dev`), file changes are visible in the container immediately. However, Dagster's gRPC code server does not auto-detect file changes. After editing code, click *
*Reload** in the Dagster UI at [http://localhost:3000](http://localhost:3000) to reload the code location.

If you prefer automatic reloads on save, use `make docker_watch` instead (Stage 1 sync+restart workflow). That triggers a container restart whenever source files change, at the cost of a few seconds
of downtime per reload.

---

## Selective service startup (profiles)

The dev overlay defines compose profiles so you can start only the services you need:

```bash
# Start only core + databases + dagster (default, no profile flag needed)
make docker_dev

# For direct Compose profile commands, first export the values printed by
# `python3 scripts/manifest_versions.py env --format shell` in your shell or Compose run configuration.
```
