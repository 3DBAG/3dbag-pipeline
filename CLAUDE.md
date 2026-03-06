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
  - `assets/export/` - Validation and format conversion
  - `assets/deploy/` - Deployment to servers
  - `assets/release/` - Final release publishing

- **packages/floors_estimation/** - ML-based floor count prediction workflow (separate due to heavy sklearn dependencies)

- **packages/party_walls/** - Party wall calculation workflow (separate due to urban-morphology-3d dependency)

Each package has its own `pyproject.toml`, dependencies, tests, and Dagster code location.

### Docker Architecture

Six services orchestrated via docker/compose.yaml:
- **dagster-postgresql** - Dagster's event/run/schedule storage
- **data-postgresql** - Pipeline data storage
- **bag3d-core**, **bag3d-party-walls**, **bag3d-floors-estimation** - Workflow execution containers
- **dagster-webserver** - Dagster UI and daemon

Docker images published to DockerHub as 3dgi/* with image tag controlling development/release versions.

### Dagster Code Locations

Each workflow package registers a Dagster code location (files named `code_location.py`) that defines:
- Definitions (assets, jobs, resources)
- Job definitions for orchestrating asset groups
- Resource configuration for the workflow

The `dagster/workspace.yaml` registers all code locations and `dagster/dagster.yaml` configures Dagster's run/event storage to use PostgreSQL.

## Development Commands

All commands use `uv` (modern Python package manager). Docker is required for testing.

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

### Code Quality (Ruff)

```bash
# Apply formatting and run lint checks (modifies files)
make format

# Manual formatting (without make)
uv tool run ruff format ./packages
uv tool run ruff check ./packages
```

Ruff is configured for line-length 88, double quotes, and space indentation. Config is in pyproject.toml `[tool.ruff]` section.

### Docker Setup and Teardown

```bash
# Create volumes and start all services with build
make docker_up

# Start only PostgreSQL (faster for small tests)
make docker_up_postgres

# Start services without rebuilding images
make docker_up_nobuild

# Watch for source changes (with hot reload)
make docker_watch

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

Tests run inside Docker containers. Database and execution context are provided automatically.

```bash
# Download test data first (required once)
make download

# Run all tests (standard unit tests only)
make test

# Run tests including slow tests
make test_slow

# Run integration tests (full workflows, slower)
make test_integration

# Run deployment tests (full end-to-end workflows)
make test_deploy

# Run all test variants
make test_all

# Parse test results from log file
make test_report
```

Tests are organized per package:
- `packages/common/tests/` - Common package tests (database, geodata, requests, resources, types)
- `packages/core/tests/` - Core workflow tests (per-asset-group plus integration)
- `packages/floors_estimation/tests/` - Floors estimation tests
- `packages/party_walls/tests/` - Party walls tests

Test markers:
- `--run-slow` - Include slow tests (`@pytest.mark.slow`)
- `--run-all` - Include tests needing local tool builds (`@pytest.mark.needs_tools`)
- `--run-deploy` - Include deployment tests (`@pytest.mark.needs_deploy`)

Single test execution from inside a container:
```bash
docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/test_assets_ahn.py::test_function_name -v
```

### Versioning and Releases

Version follows `YYYY.0M.0D` format (date-based). Configured via bumpver in pyproject.toml:

```bash
# Bump version and auto-commit/tag/push
bumpver update --patch  # Updates all 5 pyproject.toml files automatically
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
def my_asset(context, config: MyAssetConfig, production_db: DatabaseResource):
    if config.force_recompute:
        # ...
```

### Resource Usage

Assets use **pythonic resources** with typed parameters. Resources are injected directly as function parameters using type hints:

```python
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.files import FileStoreResource

@asset
def my_asset(context, production_db: DatabaseResource, file_store: FileStoreResource):
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
- **GeoflowResource** - 3D geometry processing
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
- `conftest.py` provides fixtures (database setup, resources, paths)
- Integration tests marked with `@pytest.mark.integration`
- Slow tests marked with `@pytest.mark.slow`
- Tests requiring tool builds marked with `@pytest.mark.needs_tools`
- Test data stored in `tests/test_data/` (use `make download` to fetch)

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

- **Local development:** `.env` file (not committed, required by makefile for `make download` target)
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

Jobs are defined in `packages/core/src/bag3d/core/jobs.py` using `define_asset_job` with explicit `AssetSelection`. Register in `code_location.py`'s `all_jobs` list.

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
