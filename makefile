SHELL := /bin/bash
.SHELLFLAGS := -ec

.DEFAULT_GOAL := help

include docker/.env

export COMPOSE_PROJECT_NAME := $(if $(COMPOSE_PROJECT_NAME),$(COMPOSE_PROJECT_NAME),bag3d-dev)
export BAG3D_DOCKER_IMAGE_TAG := $(if $(BAG3D_DOCKER_IMAGE_TAG),$(BAG3D_DOCKER_IMAGE_TAG),develop)

.PHONY: help \
	docker_up docker_up_postgres docker_up_nobuild docker_dev docker_build \
	docker_restart docker_restart_containers docker_down docker_down_rm docker_prune_cache \
	docker_volume_create docker_volume_create_data_postgresql docker_volume_create_data_pipeline \
	docker_volume_create_dagster_home docker_volume_create_dagster_postgresql \
	docker_volume_rm docker_volume_recreate \
	test test_report lint lint_fix \
	local_install_uv local_venv local_dev

help:
	@echo "3dbag-pipeline - Available targets:"
	@echo ""
	@echo "Docker Management:"
	@echo "  docker_up                    Start all Docker services with build"
	@echo "  docker_up_postgres           Start only PostgreSQL (faster for small tests)"
	@echo "  docker_up_nobuild            Start services without rebuilding images"
	@echo "  docker_dev                   Start services with dev overrides"
	@echo "  docker_build                 Rebuild all Docker images without cache"
	@echo "  docker_restart               Stop, recreate volumes, and start fresh"
	@echo "  docker_restart_containers    Restart running containers (keep volumes)"
	@echo "  docker_down                  Stop all services"
	@echo "  docker_down_rm               Stop and remove volumes/images"
	@echo "  docker_prune_cache           Prune Docker builder cache"
	@echo ""
	@echo "Testing:"
	@echo "  test                         Run unit tests (fast, offline, no Docker)"
	@echo "  test_report                  Parse and summarize test results"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint                         Check formatting, style, and types"
	@echo "  lint_fix                     Apply automatic formatting and fixes"
	@echo ""
	@echo "Development:"
	@echo "  local_install_uv             Install uv package manager"
	@echo "  local_venv                   Create virtualenvs for all packages"
	@echo "  local_dev                    Start Dagster dev server locally (no Docker)"
	@echo ""

docker_volume_create_data_postgresql:
	docker volume create $(BAG3D_DOCKER_VOLUME_DATA_POSTGRESQL)
	docker run -d --name $(TEMP_CONTAINER) --mount source=$(BAG3D_DOCKER_VOLUME_DATA_POSTGRESQL),target=/data busybox sleep infinity
	docker exec $(TEMP_CONTAINER) mkdir -p /data/pgdata /data/pglog
	docker rm -f $(TEMP_CONTAINER)

docker_volume_create_data_pipeline:
	docker volume create $(BAG3D_DOCKER_VOLUME_DATA_PIPELINE)

docker_volume_create_dagster_home:
	docker volume create $(BAG3D_DOCKER_VOLUME_DAGSTER_HOME)
	docker run -d --name $(TEMP_CONTAINER) --mount source=$(BAG3D_DOCKER_VOLUME_DAGSTER_HOME),target=/opt/dagster/dagster_home busybox sleep infinity
	docker cp docker/dagster/dagster.yaml $(TEMP_CONTAINER):/opt/dagster/dagster_home/
	docker cp docker/dagster/workspace.yaml $(TEMP_CONTAINER):/opt/dagster/dagster_home/
	docker rm -f $(TEMP_CONTAINER)

docker_volume_create_dagster_postgresql:
	docker volume create $(BAG3D_DOCKER_VOLUME_DAGSTER_POSTGRESQL)

docker_volume_create: docker_volume_create_dagster_home docker_volume_create_dagster_postgresql docker_volume_create_data_postgresql docker_volume_create_data_pipeline

docker_volume_rm:
	docker volume rm -f $(BAG3D_DOCKER_VOLUME_DATA_POSTGRESQL)
	docker volume rm -f $(BAG3D_DOCKER_VOLUME_DATA_PIPELINE)
	docker volume rm -f $(BAG3D_DOCKER_VOLUME_DAGSTER_HOME)
	docker volume rm -f $(BAG3D_DOCKER_VOLUME_DAGSTER_POSTGRESQL)

docker_volume_recreate: docker_volume_rm docker_volume_create

docker_up_postgres:
	docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml up -d --wait data-postgresql

docker_up:
	docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml up -d

docker_up_nobuild:
	docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml up -d --no-build

docker_dev:
	docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml -f docker/compose.dev.yaml up -d

docker_build:
	docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml build --no-cache

docker_restart: docker_down docker_volume_recreate docker_up

docker_restart_containers:
	docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml restart


docker_down:
	docker compose -p $(COMPOSE_PROJECT_NAME) down --remove-orphans

docker_down_rm:
	docker compose -p $(COMPOSE_PROJECT_NAME) down --volumes --remove-orphans --rmi local

docker_prune_cache:
	docker builder prune --filter type=exec.cachemount

test:
	@set -o pipefail; \
	FAILED=0; \
	uv --project packages/common run pytest packages/common/tests/ -v || FAILED=1; \
	uv --project packages/core run pytest packages/core/tests/ -v || FAILED=1; \
	uv --project packages/export run pytest packages/export/tests/ -v || FAILED=1; \
	uv --project packages/floors_estimation run pytest packages/floors_estimation/tests/ -v || FAILED=1; \
	uv --project packages/party_walls run pytest packages/party_walls/tests/ -v || FAILED=1; \
	exit $$FAILED

test_report:
	python3 scripts/parse_test_log.py

lint:
	@set -e; set -o pipefail; \
	echo "Manifest version check"; \
	python3 scripts/check_manifest_versions.py; \
	echo "Format check"; \
	uv tool run ruff format --check ./packages; \
	echo "Syntax and style"; \
	uv tool run ruff check ./packages; \
	FAILED=0; \
	echo "Type check: common"; \
	uv --project packages/common run pyright packages/common || FAILED=1; \
	echo "Type check: core"; \
	uv --project packages/core run pyright packages/core || FAILED=1; \
	echo "Type check: export"; \
	uv --project packages/export run pyright packages/export || FAILED=1; \
	echo "Type check: floors_estimation"; \
	uv --project packages/floors_estimation run pyright packages/floors_estimation || FAILED=1; \
	echo "Type check: party_walls"; \
	uv --project packages/party_walls run pyright packages/party_walls || FAILED=1; \
	exit $$FAILED

lint_fix:
	uv tool run ruff format ./packages
	uv tool run ruff check --fix ./packages

local_install_uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh

local_venv:
	uv sync
	uv --project packages/common sync
	uv --project packages/core sync
	uv --project packages/export sync
	uv --project packages/floors_estimation sync
	uv --project packages/party_walls sync

local_dev:
	uv run dagster dev -w tests/dagster_home/workspace.yaml
