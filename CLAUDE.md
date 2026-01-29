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

The dagster/workspace.yaml registers all code locations and the dagster/dagster.yaml configures Dagster's run/event storage to use PostgreSQL.

## Development Commands

All commands use `uv` (modern Python package manager). Docker is required for testing.

### Python Package Management

```bash
# Install dev dependencies into root .venv (includes dev, docs, lint groups)
uv sync

# Add dependency to root pyproject.toml
uv add --dev <package>

# Add dependency to a specific package
uv add -p packages/core --dev <package>

# Update dependencies
uv sync --upgrade

# Use a specific Python version
uv python pin 3.11
```

### Code Quality (Ruff)

```bash
# Lint and format check
make format

# Manual formatting (without make)
ruff check packages/*/src/ scripts/
ruff format packages/*/src/ scripts/ --preview
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
```

Tests are organized per package:
- `packages/common/tests/` - Common package tests (database, geodata, requests, resources, types)
- `packages/core/tests/` - Core workflow tests (per-asset-group plus integration)
- `packages/floors_estimation/tests/` - Floors estimation tests
- `packages/party_walls/tests/` - Party walls tests

Test markers:
- `--run-slow` - Include slow tests
- `--run-all` - Include all test variants (integration, slow, deploy)
- `--run-deploy` - Include deployment tests

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

# Documentation is auto-published by GitHub Actions to https://innovation.3dbag.nl/3dbag-pipeline
```

Documentation is in `docs/` and configured in mkdocs.yml with Material theme. API docs are auto-generated from docstrings.

## Key Implementation Patterns

### Dagster Assets and Definitions

Assets are defined using the `@asset` decorator and organized in `asset_groups.py`. Related assets are grouped for job creation. Each workflow package exports a `Definitions` object from `code_location.py` that includes:
- Asset groups
- Jobs (explicit orchestration of asset groups)
- Resources (database, executables, file operations)
- Sensors for automation

### Resource Usage

The `common` package provides reusable Dagster resources accessed via `context.resources`:
- `db` - Database connection and operations
- `execmd` - Execute command tool (wrapper around docker exec or subprocess)
- `specs` - Building specifications from bag3d-specs
- `wkt` - WKT geometry handling
- And others (server transfer, file operations, version management)

See `packages/common/src/bag3d/resources/` for resource implementations.

### External Tool Execution

The pipeline orchestrates external CLI tools through the `CommandRunner` (renamed from `AppImage`). This handles:
- Docker container execution
- Local subprocess execution
- Output capture and error handling
- Tool versioning management

The current branch (215-upgrade-to-dagster-pipes) is migrating to Dagster's native Pipes feature for better subprocess management. See recent commits:
- `7f08f3d` - Refactor remaining appimage calls and tests
- `9303d39` - Remove dagster-shell dependency
- `17f6f5c` - Migrate all call sites from .app.execute() to .runner.run()

### Database Design

PostgreSQL stores both:
- Dagster's internal state (runs, events, schedules) - configured in `docker/dagster/dagster.yaml`
- Pipeline data (reconstructed buildings, metadata, validation results) - in `data-postgresql` service

Database initialization and schema management happens via SQL files in `packages/common/src/bag3d/sqlfiles/`.

### Testing Pattern

Tests use pytest with these conventions:
- Unit tests in `tests/` subdirectory per package
- `conftest.py` provides fixtures (database setup, resources, paths)
- Integration tests marked with `@pytest.mark.integration`
- Slow tests marked with `@pytest.mark.slow`
- Test data stored in `tests/test_data/` (use `make download` to fetch if needed)

## Configuration

### Environment Variables

- **Local development:** `.env` file (not committed, see `.env.example` if it exists)
- **Docker services:** `docker/.env` (committed, contains volume names and PostgreSQL credentials)
- **Dagster home:** `tests/dagster_home/` contains `dagster.yaml` and `workspace.yaml`

### Package-Specific Configuration

Each package has its own `pyproject.toml`:
- `packages/common/pyproject.toml` - Shared utilities and resources
- `packages/core/pyproject.toml` - Core workflow dependencies
- `packages/floors_estimation/pyproject.toml` - Heavy sklearn dependencies
- `packages/party_walls/pyproject.toml` - Spatial computation dependencies

Root `pyproject.toml` defines development dependencies and build tools (bumpver, ruff, mkdocs).

### Dagster Configuration

- `docker/dagster/dagster.yaml` - Dagster daemon config, PostgreSQL storage, workspace location
- `docker/dagster/workspace.yaml` - Code location definitions (points to each package's `code_location.py`)
- Deployed to `DAGSTER_HOME` volume via makefile target

## File Structure Reference

Key files to know:

- `packages/*/src/bag3d/` - Source code root for each package (follows Python packaging convention)
- `packages/*/tests/conftest.py` - Pytest fixtures and test setup
- `packages/*/code_location.py` - Dagster definitions exported for orchestration
- `packages/*/asset_groups.py` - Asset grouping for job creation
- `docker/compose.yaml` - Service definitions and orchestration
- `docker/.env` - Service configuration (volumes, credentials)
- `.github/workflows/` - CI/CD pipelines (build, test, lint, release, docs)
- `scripts/` - Utility scripts (monitoring, analysis, database admin)

## Common Workflows

### Adding a New Asset

1. Create asset function with `@asset` decorator in appropriate `assets/*/` module
2. Add to relevant asset group in `asset_groups.py`
3. Define job if orchestration changes needed
4. Add tests in `tests/` directory
5. Document in docstring (auto-included in API docs)

### Modifying Pipeline Execution

Pipeline behavior is defined by:
- Asset dependencies (implicit via inputs)
- Job definitions (explicit via `define_asset_job`)
- Sensors and schedules (automated triggers)
- Resources (execution context)

Changing execution typically requires modifying `jobs.py` or asset group membership.

### Running Local Development

```bash
# Full setup
make docker_up
# OR quick PostgreSQL setup
make docker_up_postgres

# Then run tests
make test

# Or use dagster-webserver for manual testing:
# 1. Access http://localhost:3000
# 2. Load code locations from workspace
# 3. Manually launch asset selections/jobs
```

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

All external tools are containerized and versioned via Docker image tags. Tool paths and versions are managed via resources in `packages/common/src/bag3d/resources/executables.py`.

## Current Development Focus

The current branch is upgrading to Dagster Pipes for subprocess management (see recent commits). This replaces the previous shell-based execution pattern with Dagster's native Pipes feature for better error handling and resource management.

## License

Dual licensed under Apache 2.0 and MIT. See LICENSE-APACHE and LICENSE-MIT files.