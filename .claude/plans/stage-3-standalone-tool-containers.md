# Stage 3: Run Tools in Standalone Docker Containers

## Goal

Decouple external tools (roofer, tyler, GDAL, PDAL, LAStools, val3dity, cjval, geoflow) from the pipeline images. Each tool runs in its own lightweight Docker container, invoked from within asset code via the Docker API. This eliminates the 3GB+ `3dbag-pipeline-tools` base image, dramatically reduces build times, and makes tool version management explicit.

---

## Current State

### How tools are invoked today

All tool calls go through `CommandRunner` in `packages/common/src/bag3d/common/resources/executables.py`:

```
Asset → Resource.runner → CommandRunner.run(cmd_template, exe_name, local_path, ...)
  → _run_with_logging()  [subprocess.Popen, shell=True]
  or → _run_docker()      [docker SDK, only for GDAL/PDAL when exe paths not set]
```

The `_run_docker()` backend (line 171) already exists and works for GDAL and PDAL. It uses `docker.from_env()`, `client.containers.run(image, command, detach=True, network_mode="host", volumes={...})`, waits for exit, reads logs, and removes the container.

### Key patterns in asset code

- Commands are shell strings with `{exe}` and `{local_path}` placeholders
- Database connections are passed as `PG:"<dsn>"` in command arguments
- Env vars (e.g., `RAYON_NUM_THREADS`) are prepended to the command string
- File paths reference the container filesystem (`/data/volume/...`)
- Some tools reference other tools' paths (tyler calls geoflow via `--exe-geof`)
- Validation tools run in a `ProcessPoolExecutor` without Dagster logger

### Tool images available

| Tool | Existing Docker image | Currently compiled from source? |
|---|---|---|
| roofer | `3dgi/roofer:develop` | No (copied from image in tools Dockerfile) |
| tyler | `3dgi/tyler:0.3.14` | No (copied from image) |
| tyler-multiformat | `3dgi/tyler-multiformat:0.4.0-alpha10` | No (copied from image) |
| tyler-db | `3dgi/tyler-db:0.3.4-db-alpha5` | No (copied from image) |
| GDAL/ogr2ogr | `ghcr.io/osgeo/gdal:ubuntu-small-latest` | Yes (compiled in tools) |
| PDAL | `pdal/pdal:sha-cfa827b6` | Yes (compiled in tools) |
| LAStools | — | Yes (compiled in tools) |
| val3dity | — | Yes (compiled in tools) |
| cjval | `tudelft3d/cjval:0.8.2` | No (copied from image) |
| cjio | — | Yes (pip installed in tools) |
| geoflow | `3dgi/geoflow-bundle-builder:2025.09.01` | No (copied from image) |

---

## Prerequisites

### 3.0a Check PipesDockerClient log forwarding in dagster 1.12

**What:** Test whether `dagster.PipesDockerClient` in dagster 1.12 properly forwards container stdout/stderr to Dagster logs. The user reported this was broken in dagster 1.9 (libs 0.25).

**How:**
1. Create a test asset that uses `PipesDockerClient` to run a simple container (e.g., `alpine echo "hello"`)
2. Materialize it in the Dagster UI
3. Check if "hello" appears in the structured event logs (not just compute logs)
4. Test with a multi-line output and stderr to verify completeness

**Decision point:** If PipesDockerClient works, use it as the primary interface (better Dagster integration, structured logging). If not, continue using the `docker` SDK directly via `CommandRunner._run_docker()`.

### 3.0b Docker socket on pipeline containers

The pipeline containers need access to the host Docker socket to launch tool containers. This requires:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

This is already done in Stage 2's `compose.dev.yaml`. For production, the compose-prod.yaml overlay also needs this mount on `bag3d-core` (it's currently only on webserver/daemon).

### 3.0c Configurable network and volume names

Tool containers need to join the same Docker network and mount the same data volume as the pipeline containers. These names are already in env vars (`BAG3D_DOCKER_NETWORK`, `BAG3D_DOCKER_VOLUME_DATA_PIPELINE`) and must be passed through to the Docker API calls.

---

## Implementation Plan

### 3.1 Extend `CommandRunner._run_docker()` to be the general tool execution path

**File:** `packages/common/src/bag3d/common/resources/executables.py`

The existing `_run_docker()` method (line 171) is already functional but limited. Extend it:

```python
@dataclass(frozen=True)
class DockerConfig:
    """Configuration for running a tool in a Docker container."""
    image: str
    network: str | None = None          # Docker network to join
    volumes: dict[str, dict] | None = None  # {host_path_or_volume: {"bind": path, "mode": "rw"}}
    environment: dict[str, str] | None = None  # env vars for the container
    working_dir: str | None = None
```

Changes to `_run_docker()`:
- Accept `network` parameter (currently hardcoded to `network_mode="host"`) — use env var `BAG3D_DOCKER_NETWORK` as default
- Accept `volumes` parameter — default to mounting `BAG3D_DOCKER_VOLUME_DATA_PIPELINE` at `/data/volume`
- Accept `environment` parameter — for tool-specific env vars like `RAYON_NUM_THREADS`
- Forward stdout/stderr to Dagster logger in real-time (stream container logs instead of reading after exit)
- Add timeout support

### 3.2 Make each tool resource support Docker mode

Currently only `GDALResource` and `PDALResource` have Docker image config. Extend all resource classes with optional Docker image fields:

```python
class RooferResource(ConfigurableResource):
    exe_roofer: str = ""
    exe_crop: str = ""
    # New: Docker mode config
    docker_image: str = "3dgi/roofer:develop"
    docker_enabled: bool = False

    @property
    def runner(self) -> CommandRunner:
        if self.docker_enabled:
            return CommandRunner(
                exes={"roofer": "roofer", "crop": "crop"},  # in-container paths
                docker_config=DockerConfig(image=self.docker_image),
            )
        return CommandRunner(exes={"roofer": self.exe_roofer, "crop": self.exe_crop})
```

The `docker_enabled` flag lets us gradually migrate tool by tool. When enabled, `exe_name` maps to the in-container binary path, and `local_path` is volume-mounted.

### 3.3 Handle the tyler + geoflow cross-tool dependency

Tyler calls geoflow via `--exe-geof <path>`. In the current setup, both binaries exist in the same container. With standalone containers, two approaches:

**Option A: Bundle tyler + geoflow in the same container.** Create a `3dgi/tyler-bundle` image that includes both tyler and geoflow. This is the simplest approach and preserves the current command structure.

**Option B: Tyler container mounts geoflow from a shared volume.** Install geoflow into a named volume, mount it into the tyler container. More complex but keeps images independent.

**Decision:** Option A. Tyler and geoflow are tightly coupled (tyler invokes geoflow per-tile). The combined image is still small compared to the current tools image.

### 3.4 Handle validation tools in ProcessPoolExecutor

The `check_formats()` function in `packages/core/src/bag3d/core/assets/export/validate.py` runs val3dity, cjval, and cjio in a `ProcessPoolExecutor` with a bare `CommandRunner()` (no Dagster logger). When switching to Docker mode:

- The `CommandRunner._run_docker()` path doesn't need a Dagster logger — it can collect stdout/stderr and return them in `CommandResult`
- The ProcessPoolExecutor pattern still works: each worker process creates its own Docker client and launches a container
- May need to limit concurrency to avoid overwhelming the Docker daemon (too many simultaneous containers)

### 3.5 Create lightweight pipeline base image

Once tools are no longer bundled, the pipeline images don't need the 3GB `3dbag-pipeline-tools` base. Create a new lightweight base:

**`docker/pipeline/base.dockerfile`:**
```dockerfile
FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.8.2 /uv /usr/local/bin/uv

# Install system dependencies needed by Python packages (psycopg, lxml, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline
ENV VIRTUAL_ENV=$BAG3D_PIPELINE_LOCATION/venv
RUN uv venv --python 3.12 $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

# Install dagster
COPY docker/tools/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install -r /tmp/requirements.txt
```

This image would be ~200-300MB instead of 3GB+. The pipeline Dockerfiles would change their `FROM` line:

```dockerfile
# Before:
FROM 3dgi/3dbag-pipeline-tools:2026.02.13 AS develop

# After:
FROM 3dgi/3dbag-pipeline-base:2026.xx.xx AS develop
```

### 3.6 Environment variable configuration for Docker tool execution

Add new env vars to `docker/.env`:

```ini
# Tool execution mode: "local" (subprocess) or "docker" (standalone containers)
BAG3D_TOOL_EXECUTION_MODE=local

# Docker images for tools (used when BAG3D_TOOL_EXECUTION_MODE=docker)
BAG3D_DOCKER_IMAGE_ROOFER=3dgi/roofer:develop
BAG3D_DOCKER_IMAGE_TYLER=3dgi/tyler:0.3.14
BAG3D_DOCKER_IMAGE_TYLER_MULTIFORMAT=3dgi/tyler-multiformat:0.4.0-alpha10
BAG3D_DOCKER_IMAGE_TYLER_DB=3dgi/tyler-db:0.3.4-db-alpha5
BAG3D_DOCKER_IMAGE_GDAL=ghcr.io/osgeo/gdal:ubuntu-small-3.8.5
BAG3D_DOCKER_IMAGE_PDAL=pdal/pdal:2.8.4
BAG3D_DOCKER_IMAGE_CJVAL=tudelft3d/cjval:0.8.2
BAG3D_DOCKER_IMAGE_GEOFLOW=3dgi/geoflow-bundle-builder:2025.09.01
```

Resource configuration in `code_location.py` reads these and configures `docker_enabled` + `docker_image` on each tool resource.

---

## Migration Strategy: Tool by Tool

Don't migrate all tools at once. Migrate one at a time, validate, then proceed:

### Phase A: GDAL/ogr2ogr (already partially supported)

`_run_docker()` already works for GDAL. Validate that:
- BGT load (`assets/bgt/load.py`) works with Docker GDAL
- BAG load (`assets/bag/load.py`) works with Docker GDAL
- sozip export works with Docker GDAL
- `PG:"<dsn>"` connections work from within the tool container (network access to `data-postgresql`)

### Phase B: PDAL (already partially supported)

Validate that:
- AHN metadata extraction (`assets/ahn/metadata.py`) works with Docker PDAL
- LAStools operations (lasindex, las2las, lasinfo) — these don't have official images. Either create a small `3dgi/lastools` image or keep them compiled in the base image.

### Phase C: Roofer

The most critical tool. Validate:
- Reconstruction (`assets/reconstruction/reconstruction.py`) works with Docker roofer
- TOML config file paths are correctly mapped
- LAZ file paths (from database) are accessible from within the roofer container
- Output directory is writable and accessible by the pipeline container after roofer exits

### Phase D: Tyler + Geoflow bundle

Create the combined tyler+geoflow image. Validate:
- `TYLER_RESOURCES_DIR` and `GF_PLUGIN_FOLDER` are correctly set in the container
- Environment vars (`RAYON_NUM_THREADS`, `RUST_LOG`) are passed through
- Tyler's `--exe-geof` flag points to the geoflow binary inside the container

### Phase E: Validation tools (val3dity, cjval, cjio)

These run in ProcessPoolExecutor. Validate:
- Concurrent container launches don't overwhelm Docker
- stdout/stderr are correctly captured without Dagster logger
- Performance is acceptable (container startup overhead × many validation runs)

### Phase F: Remove tools from base image

Once all tools are migrated:
1. Switch pipeline Dockerfiles to the lightweight base image
2. Remove the tools build from the `3dgi/3dbag-pipeline-tools` Dockerfile (or archive it)
3. Update GH Actions workflows (build-docker-tools.yaml may become unnecessary)
4. Update `EXE_PATH_*` env vars to be empty/removed when in Docker mode

---

## Key Files to Modify

| File | Change |
|---|---|
| `packages/common/src/bag3d/common/resources/executables.py` | Extend `DockerConfig`, `CommandRunner._run_docker()`, all tool resources |
| `packages/core/src/bag3d/core/code_location.py` | Resource configuration for Docker mode |
| `packages/floors_estimation/src/bag3d/floors_estimation/code_location.py` | Same |
| `packages/party_walls/src/bag3d/party_walls/code_location.py` | Same |
| `docker/.env` | New `BAG3D_TOOL_EXECUTION_MODE` and `BAG3D_DOCKER_IMAGE_*` vars |
| `docker/compose.yaml` | Docker socket mount on pipeline containers |
| `docker/compose.dev.yaml` | Same |
| `docker/pipeline/base.dockerfile` (new) | Lightweight Python-only base image |
| `docker/pipeline/bag3d-core.dockerfile` | Change FROM to lightweight base |
| `docker/pipeline/bag3d-floors-estimation.dockerfile` | Same |
| `docker/pipeline/bag3d-party-walls.dockerfile` | Same |
| Deployment compose overlays in `3dbag-admin` | Docker socket mount, tool image env vars |

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Container startup overhead per tool call | Slower execution, especially for many small calls (validation) | Benchmark first; for high-frequency calls, consider keeping the tool in the base image |
| Network connectivity between tool containers and data-postgresql | Tools can't access DB | Use `network` parameter to join `bag3d-network`; test PG connectivity from tool containers |
| Volume mount path mapping complexity | Wrong file paths in tool commands | `CommandRunner._run_docker()` already handles path remapping; extend as needed |
| Docker-in-Docker stability | Reliability issues | We're using Docker socket mounting (DooD), not nested Docker — this is well-supported |
| Tyler+geoflow tight coupling | Complex container setup | Bundle in single image (Option A above) |
| PipesDockerClient doesn't forward logs | No structured Dagster logging | Fall back to `docker` SDK with `_run_docker()` (already working) |

---

## Verification

### Per-tool validation
For each migrated tool:
1. Run the corresponding `make test` target with `BAG3D_TOOL_EXECUTION_MODE=docker`
2. Run the corresponding Dagster job in the UI and verify:
   - Tool output appears in Dagster logs
   - Output files are created correctly
   - No file permission issues

### End-to-end validation
1. Run `make test_integration` with all tools in Docker mode
2. Compare output artifacts with a baseline run using local tools
3. Measure execution time overhead from container startup

### Image size validation
1. Build the lightweight base image: `docker build -f docker/pipeline/base.dockerfile -t 3dgi/3dbag-pipeline-base:test .`
2. Build a pipeline image on top: `docker build -f docker/pipeline/bag3d-core.dockerfile -t test-core .`
3. Compare `docker images` sizes: expect ~300MB vs current ~3.5GB
4. Verify GH Actions build time for the pipeline images (should be minutes, not 30+)
