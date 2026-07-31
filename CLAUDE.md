# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

3dbag-pipeline is a data orchestration pipeline built with [Dagster](https://dagster.io) that orchestrates the complete 3D Building Address Group (3DBAG) data production workflow. The pipeline:
- Ingests and preprocesses source data (AHN, BAG, BGT, TOP10NL)
- Prepares input data (tiling, intermediary processing)
- Reconstructs buildings in 3D
- Validates, converts formats, and exports results
- Deploys data to production servers
- Provides auxiliary workflows (floor estimation, party wall calculation)

The pipeline is written in Python and orchestrates external tools (roofer, tyler, GDAL, PDAL) running in Docker containers.

## Architecture Overview

### Monorepo Structure

The project uses a monorepo with independent packages to avoid dependency conflicts:

- **packages/common/** - Shared resources, utilities, and type definitions used by all workflows. Contains Dagster resources (database, executables, file operations, server transfer), utility modules for geodata/requests/database operations, SQL templates, and shared types.

- **packages/core/** - Main 3DBAG production workflow with assets organized by data source:
  - `assets/ahn/` - AHN height model data (download, core extraction, indexing, metadata)
  - `assets/bag/` - BAG building footprint data (download, load)
  - `assets/bgt/` - BGT topographic data (download, load)
  - `assets/top10nl/` - TOP10NL geographic data (download, load)
  - `assets/input/` - Input preparation (tiling, intermediary processing, reconstruction inputs)
  - `assets/reconstruction/` - 3D building reconstruction using roofer

- **packages/export/** - Export, deploy, and release workflow (split from core for independent scaling):
  - `assets/export/` - Validation and format conversion
  - `assets/deploy/` - Deployment to publication server
  - `assets/release/` - Final release publishing

- **packages/floors_estimation/** - ML-based floor count prediction workflow (separate due to heavy sklearn dependencies)

- **packages/party_walls/** - Party wall calculation workflow (separate due to urban-morphology-3d dependency)

Each package has its own `pyproject.toml`, dependencies, tests, and Dagster code location.

### Docker Architecture

Services orchestrated via docker/compose.yaml:
- **dagster-postgresql** - Dagster's event/run/schedule storage
- **data-postgresql** - Pipeline data storage
- **bag3d-core**, **bag3d-export**, **bag3d-party-walls**, **bag3d-floors-estimation** - Workflow execution containers (gRPC code locations on ports 4000-4003)
- **dagster-webserver** - Dagster UI
- **dagster-daemon** - Dagster daemon (schedules, sensors, run queuing)

For development with live source code bind-mounts, use `docker/compose.dev.yaml` overlay via `make docker_dev`.

Docker images published to DockerHub as 3dgi/* with image tag controlling development/release versions.

### Dagster Code Locations

Each workflow package registers a Dagster code location (files named `code_location.py`) that defines:
- Definitions (assets, jobs, resources)
- Job definitions for orchestrating asset groups
- Resource configuration for the workflow

Two workspace configurations exist:
- `docker/dagster/workspace.yaml` - Production: connects to code locations via gRPC servers
- `tests/dagster_home/workspace.yaml` - Local dev: loads code locations directly via `python_file`

Both register all code locations. `dagster/dagster.yaml` configures Dagster's run/event storage to use PostgreSQL.

## Development Commands

All commands use `uv` (modern Python package manager). Docker is optional for testing.

### Python Package Management

```bash
# Install dev dependencies into root .venv (includes dev, docs, lint groups)
uv sync

# Add dependency to a specific package
uv add -p packages/core --dev <package>

# Update dependencies
uv sync --upgrade
```

### Local Development (without Docker)

```bash
# Install uv (if not already installed)
make local_install_uv

# Create virtualenvs for all packages
make local_venv

# Run the Dagster dev server locally (uses tests/dagster_home/workspace.yaml)
make local_dev
```

`make local_dev` starts Dagster at http://localhost:3000 using the local workspace config, without requiring Docker containers for the workflow services.

### Code Quality (Ruff + Pyright)

```bash
# Full lint: format, style check, and type check all packages
make lint

# Auto-fix: format and apply automatic lint fixes
make lint_fix
```

`make lint` runs ruff format, ruff check, and pyright type checking on all packages. Ruff is configured for line-length 88, double quotes, and space indentation. Config is in pyproject.toml `[tool.ruff]` section.

### Docker Setup and Teardown

```bash
# Create volumes and start all services with build
make docker_up

# Start only PostgreSQL (faster for small tests)
make docker_up_postgres

# Start services without rebuilding images
make docker_up_nobuild

# Start services with dev overrides (bind-mounts for live editing)
make docker_dev

# Rebuild all Docker images without cache
make docker_build

# Full restart: stop everything, recreate volumes, start fresh
make docker_restart

# Stop all services
make docker_down

# Stop and remove volumes/images
make docker_down_rm

# Restart running containers (keep volumes)
make docker_restart_containers
```

### Testing

Tests run locally with mocked resources by default. Docker is only needed for full pipeline development.

```bash
# Run all tests
make test

# Parse test results from log file
make test_report
```

Tests are organized per package:
- `packages/common/tests/` - Common package tests (database, geodata, requests, resources, types)
- `packages/core/tests/` - Core workflow tests (per-asset-group plus integration)
- `packages/export/tests/` - Export workflow tests
- `packages/floors_estimation/tests/` - Floors estimation tests
- `packages/party_walls/tests/` - Party walls tests

Single test execution (locally):
```bash
uv --project packages/core run pytest packages/core/tests/test_assets_ahn.py::test_function_name -v
```

### Versioning and Releases

Version follows `YYYY.0M.0D` format (date-based). Configured via bumpver in pyproject.toml:

```bash
# Bump version and auto-commit/tag/push
bumpver update --patch  # Updates all 6 pyproject.toml files automatically
```

The `.github/hooks/pre-commit` hook runs before commits.

### Documentation

```bash
# Build MkDocs site locally
mkdocs serve

# Build for production
mkdocs build
```

Documentation is in `docs/` and configured in mkdocs.yml with Material theme. API docs are auto-generated from docstrings. Auto-published to https://innovation.3dbag.nl/3dbag-pipeline via GitHub Actions.

## Key Implementation Patterns

### Dagster Assets and Definitions

Assets are defined using the `@asset` decorator and organized in `asset_groups.py`. Related assets are grouped for job creation using `load_assets_from_package_module` with a `key_prefix` and `group_name`. Each workflow package exports a `Definitions` object from `code_location.py` that includes:
- Asset groups
- Jobs (explicit orchestration of asset groups)
- Resources (database, executables, file operations)
- Sensors for automation

**Asset key convention:** Assets use the key format `["group_name", "asset_name"]` (e.g., `AssetKey(["ahn", "md5_ahn3"])`). Note: `@multi_asset` decorated assets do NOT inherit `key_prefix` from `load_assets_from_package_module` — they must explicitly set their keys.

**Asset Configuration:**
Assets can accept configuration using Pydantic `Config` classes:

```python
from dagster import Config
from pydantic import Field

class MyAssetConfig(Config):
    force_recompute: bool = Field(default=False, description="Force recompute even if data exists")

@asset
def my_asset(context, config: MyAssetConfig, computation_db: DatabaseResource):
    if config.force_recompute:
        # ...
```

### Resource Usage

Assets use **pythonic resources** with typed parameters. Resources are injected directly as function parameters using type hints:

```python
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.files import FileStoreResource

@asset
def my_asset(context, computation_db: DatabaseResource, file_store: FileStoreResource):
    data = db_connectio.get_dict(query)
    path = file_store.file_store.data_dir
```

The `common` package provides reusable resources (all in `packages/common/src/bag3d/common/resources/`):
- **DatabaseResource** - PostgreSQL connection and database operations
- **FileStoreResource** - File system access and paths (two instances: `file_store` and `file_store_fastssd` for different storage tiers)
- **GDALResource** - GDAL/OGR tools for vector/raster processing
- **TylerResource** - 3D tile generation tool
- **RooferResource** - 3D building reconstruction tool
- **PDALResource** - Point cloud processing (LAZ/LAS files)
- **LASToolsResource** - LASTools suite (lasindex, las2las, lasinfo)
- **ServerTransferResource** - Secure file transfer to the publication server (`publication_server`)
- **DatabaseResource** (as `publication_db`) - PostgreSQL on the publication server (BAG3D_PUBLICATION_PG_* env vars)
- **ValidationResource** - Data validation tools
- **Specs3DBAGResource** - Building specifications from bag3d-specs
- **ReleaseVersionResource** / **ToolVersionsResource** - Version management

### Resource Configuration via `DAGSTER_DEPLOYMENT`

The `DAGSTER_DEPLOYMENT` environment variable controls how resources are configured:
- `default` - Resources configured at run launch (via Dagster UI run config)
- `production` / `user` / `pytest` / `pc` - Resources loaded from environment variables at startup

In `production` mode, env vars like `BAG3D_PG_HOST`, `EXE_PATH_OGR2OGR`, `BAG3D_FILESTORE`, etc. are read at import time. In Docker containers, `DAGSTER_DEPLOYMENT=pytest` is used so tests pick up resources from env vars automatically.

### Testing Pattern

Tests use pytest with these conventions:
- Unit tests in `tests/` subdirectory per package
- `conftest.py` provides shared mock fixtures for database, file stores, and other resources
- Tests are expected to run offline without Docker, databases, or external tool binaries

**Testing assets directly:**
```python
def test_my_asset(database, file_store):
    result = my_asset(build_op_context(), database, file_store)
```

**Testing assets that depend on other assets** uses `MockAssetIOManager` (defined in `packages/core/tests/conftest.py`) to provide pre-configured return values for upstream assets without materializing them.

### Sensors and Automation

The core code location uses two automation mechanisms:
- `AutomationConditionSensorDefinition` (`"automation_condition_sensor"`) - watches `AssetSelection.all()` and triggers based on `AutomationCondition` decorators on individual assets
- `ahn_checksum_sensor` - a `@multi_asset_sensor` that monitors AHN checksum assets for changes and triggers per-partition job runs only for tiles with changed checksums; uses cursor to store previous checksums as JSON

Both sensors are started (`DefaultSensorStatus.RUNNING`) only when `DAGSTER_DEPLOYMENT == "production"`, and stopped otherwise.

### Database Design

PostgreSQL stores both:
- Dagster's internal state (runs, events, schedules) - configured in `docker/dagster/dagster.yaml`
- Pipeline data (reconstructed buildings, metadata, validation results) - in `data-postgresql` service

Database initialization and schema management happens via SQL files in `packages/common/src/bag3d/common/sqlfiles/` and `packages/core/src/bag3d/core/sqlfiles/`.

## Configuration

### Environment Variables

- **Local development:** `.env` file (not committed, only needed for optional local overrides)
- **Docker services:** `docker/.env` (committed, contains volume names and PostgreSQL credentials)
- **Dagster home:** `tests/dagster_home/` contains `dagster.yaml` and `workspace.yaml`

Key environment variables:
- `DAGSTER_DEPLOYMENT` - Controls resource configuration mode (`default`, `production`, `pytest`, `user`, `pc`)
- `BAG3D_PG_HOST/PORT/USER/PASSWORD/DATABASE` - PostgreSQL connection
- `BAG3D_FILESTORE`, `BAG3D_FILESTORE_FASTSSD` - Data storage paths
- `EXE_PATH_*` - Paths to external tool executables
- `BAG3D_RELEASE_VERSION` - Current release version string

### Dagster Configuration

- `docker/dagster/dagster.yaml` - Dagster daemon config, PostgreSQL storage, workspace location
- `docker/dagster/workspace.yaml` - Code location definitions (points to each package's `code_location.py`)
- Deployed to `DAGSTER_HOME` volume via makefile target

## Common Workflows

### Adding a New Asset

1. Create asset function with `@asset` decorator in appropriate `assets/*/` module
2. Add to relevant asset group in `asset_groups.py`
3. Define job if orchestration changes needed
4. Add tests in `tests/` directory
5. Document in docstring (auto-included in API docs)

### Adding a New Job

Jobs are defined using `define_asset_job` with explicit `AssetSelection` in each package's `jobs.py` (e.g., `packages/core/src/bag3d/core/jobs.py`, `packages/export/src/bag3d/export/jobs.py`). Register in `code_location.py`'s `all_jobs` list.

### Debugging

- Dagster UI at http://localhost:3000 shows execution history, logs, and code structure
- Container logs: `docker compose logs -f <service-name>`
- Exec into container: `docker compose exec <service> bash`
- Database inspection: `docker compose exec data-postgresql psql -U bag3d_user -d bag3d`

## Dependencies and External Tools

The pipeline depends on external tools installed in Docker images:
- **roofer** - 3D building reconstruction
- **tyler** - Tile generation and processing
- **GDAL/PDAL** - Geospatial data processing
- **PostgreSQL** - Database backend

All external tools are containerized and versioned via Docker image tags. Tool paths and versions are managed via resources in `packages/common/src/bag3d/common/resources/executables.py`.

## License

Dual licensed under Apache 2.0 and MIT. See LICENSE-APACHE and LICENSE-MIT files.
