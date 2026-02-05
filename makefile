SHELL := /bin/bash
.SHELLFLAGS := -ec

include docker/.env

export COMPOSE_PROJECT_NAME := $(if $(COMPOSE_PROJECT_NAME),$(COMPOSE_PROJECT_NAME),bag3d-dev)
export BAG3D_DOCKER_IMAGE_TAG := $(if $(BAG3D_DOCKER_IMAGE_TAG),$(BAG3D_DOCKER_IMAGE_TAG),develop)

sleep_a_bit:
	sleep 2

docker_volume_create_data_pipeline:
	docker rm -f $(TEMP_CONTAINER) > /dev/null 2>&1 || true
	docker volume create $(BAG3D_DOCKER_VOLUME_DATA_PIPELINE)
	docker run -d --name $(TEMP_CONTAINER) --mount source=$(BAG3D_DOCKER_VOLUME_DATA_PIPELINE),target=/data/volume busybox sleep infinity
	docker cp ./tests/test_data/. $(TEMP_CONTAINER):/data/volume
	docker rm -f $(TEMP_CONTAINER)

docker_volume_create_data_postgresql:
	docker volume create $(BAG3D_DOCKER_VOLUME_DATA_POSTGRESQL)
	docker run -d --name $(TEMP_CONTAINER) --mount source=$(BAG3D_DOCKER_VOLUME_DATA_POSTGRESQL),target=/data busybox sleep infinity
	docker exec $(TEMP_CONTAINER) mkdir -p /data/pgdata /data/pglog
	docker rm -f $(TEMP_CONTAINER)

docker_volume_create_dagster_home:
	docker volume create $(BAG3D_DOCKER_VOLUME_DAGSTER_HOME)
	docker run -d --name $(TEMP_CONTAINER) --mount source=$(BAG3D_DOCKER_VOLUME_DAGSTER_HOME),target=/opt/dagster/dagster_home busybox sleep infinity
	docker cp docker/dagster/dagster.yaml $(TEMP_CONTAINER):/opt/dagster/dagster_home/
	docker cp docker/dagster/workspace.yaml $(TEMP_CONTAINER):/opt/dagster/dagster_home/
	docker rm -f $(TEMP_CONTAINER)

docker_volume_create_dagster_postgresql:
	docker volume create $(BAG3D_DOCKER_VOLUME_DAGSTER_POSTGRESQL)

docker_volume_create: docker_volume_create_dagster_home docker_volume_create_dagster_postgresql docker_volume_create_data_pipeline docker_volume_create_data_postgresql

docker_volume_rm:
	docker volume rm -f $(BAG3D_DOCKER_VOLUME_DATA_PIPELINE)
	docker volume rm -f $(BAG3D_DOCKER_VOLUME_DATA_POSTGRESQL)
	docker volume rm -f $(BAG3D_DOCKER_VOLUME_DAGSTER_HOME)
	docker volume rm -f $(BAG3D_DOCKER_VOLUME_DAGSTER_POSTGRESQL)

docker_volume_recreate: docker_volume_rm docker_volume_create

docker_up_postgres:
	BAG3D_DOCKER_IMAGE_TAG=$(BAG3D_DOCKER_IMAGE_TAG) docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml up -d data-postgresql
	sleep 5

docker_up:
	BAG3D_DOCKER_IMAGE_TAG=$(BAG3D_DOCKER_IMAGE_TAG) docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml up -d

docker_up_nobuild:
	BAG3D_DOCKER_IMAGE_TAG=$(BAG3D_DOCKER_IMAGE_TAG) docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml up -d --no-build

docker_watch:
	BAG3D_DOCKER_IMAGE_TAG=$(BAG3D_DOCKER_IMAGE_TAG) docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml watch

docker_build:
	BAG3D_DOCKER_IMAGE_TAG=$(BAG3D_DOCKER_IMAGE_TAG) docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml build --no-cache

docker_restart: docker_down docker_volume_recreate sleep_a_bit docker_up

docker_restart_containers:
	docker compose -p $(COMPOSE_PROJECT_NAME) -f docker/compose.yaml restart


docker_down:
	docker compose -p $(COMPOSE_PROJECT_NAME) down --remove-orphans

docker_down_rm:
	docker compose -p $(COMPOSE_PROJECT_NAME) down --volumes --remove-orphans --rmi local

test:
	@set -e; set -o pipefail; \
	rm -f tests/test.log; \
	FAILED=0; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest /opt/3dbag-pipeline/packages/common/tests/ -v 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/ -v 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/ -v 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/ -v 2>&1 | tee -a tests/test.log || FAILED=1; \
	exit $$FAILED

test_slow:
	@set -e; set -o pipefail; \
	rm -f tests/test.log; \
	FAILED=0; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest /opt/3dbag-pipeline/packages/common/tests/ -v --run-slow 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/ -v --run-slow 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/ -v --run-slow 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/ -v --run-slow 2>&1 | tee -a tests/test.log || FAILED=1; \
    exit $$FAILED

test_integration:
	@set -e; set -o pipefail; \
	rm -f tests/test.log; \
	FAILED=0; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/test_integration.py -v -s --run-all 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/test_integration.py -v -s --run-all 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/test_integration.py -v -s --run-all 2>&1 | tee -a tests/test.log || FAILED=1; \
    exit $$FAILED

test_deploy:
	@set -e; set -o pipefail; \
	rm -f tests/test.log; \
	FAILED=0; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/test_integration.py -v -s --run-all --run-deploy -k 'test_integration_deploy_release' 2>&1 | tee -a tests/test.log || FAILED=1; \
    exit $$FAILED

test_all:
	@set -e; set -o pipefail; \
	rm -f tests/test.log; \
	FAILED=0; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest /opt/3dbag-pipeline/packages/common/tests/ -v --run-slow --run-all 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/ -v --run-slow  --run-all 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/ -v --run-slow --run-all 2>&1 | tee -a tests/test.log || FAILED=1; \
	docker compose -p $(COMPOSE_PROJECT_NAME) exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/ -v --run-slow --run-all 2>&1 | tee -a tests/test.log || FAILED=1; \
    exit $$FAILED

test_report:
	python3 scripts/parse_test_log.py

include .env

download:
	rm -rf $(BAG3D_TEST_DATA)
	mkdir -p $(BAG3D_TEST_DATA)
	cd $(BAG3D_TEST_DATA) ; curl -O https://data.3dbag.nl/testdata/pipeline/test_data_v14.zip ; unzip -q test_data_v14.zip ; rm test_data_v14.zip


install_uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh

format:
	uv tool run ruff format ./packages
	uv tool run ruff check ./packages

docker_build_tools:
	rm docker_build_tools.log || true
	docker buildx build --build-arg JOBS=$(BAG3D_TOOLS_DOCKERIMAGE_JOBS) --build-arg VERSION=$(BAG3D_TOOLS_DOCKERIMAGE_VERSION) --progress plain -t "$(BAG3D_TOOLS_DOCKERIMAGE):$(BAG3D_TOOLS_DOCKERIMAGE_VERSION)" -f "$(BAG3D_TOOLS_DOCKERFILE)" . >> docker_build_tools.log 2>&1
