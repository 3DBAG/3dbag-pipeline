include .env

.PHONY: download build_tools test

source:
	set -a ; . ./.env ; set +a

download: source
	rm -rf $(BAG3D_TEST_DATA)
	mkdir -p $(BAG3D_TEST_DATA)
	cd $(BAG3D_TEST_DATA) ; curl -O https://data.3dbag.nl/testdata/pipeline/test_data_v4.zip ; unzip -q test_data_v5.zip ; rm test_data_v4.zip

docker_volume_create:
	docker rm -f bag3d-dev-temp-container > /dev/null 2>&1 || true
	docker volume create bag3d-dev-data-pipeline
	docker run -d --name bag3d-dev-temp-container --mount source=bag3d-dev-data-pipeline,target=/data/volume busybox sleep infinity
	docker cp ./tests/test_data/. bag3d-dev-temp-container:/data/volume
	docker rm -f bag3d-dev-temp-container
	docker volume create bag3d-dev-data-postgresql
	docker run -d --name bag3d-dev-temp-container --mount source=bag3d-dev-data-postgresql,target=/data busybox sleep infinity
	docker exec bag3d-dev-temp-container mkdir -p /data/pgdata /data/pglog
	docker rm -f bag3d-dev-temp-container
	docker volume create bag3d-dev-dagster-home
	docker run -d --name bag3d-dev-temp-container --mount source=bag3d-dev-dagster-home,target=/opt/dagster/dagster_home busybox sleep infinity
	docker cp docker/dagster/dagster.yaml bag3d-dev-temp-container:/opt/dagster/dagster_home/
	docker cp docker/dagster/workspace.yaml bag3d-dev-temp-container:/opt/dagster/dagster_home/
	docker rm -f bag3d-dev-temp-container
	docker volume create bag3d-dev-dagster-postgresql

docker_volume_rm:
	docker volume rm -f bag3d-dev-data-pipeline
	docker volume rm -f bag3d-dev-data-postgresql
	docker volume rm -f bag3d-dev-dagster-home
	docker volume rm -f bag3d-dev-dagster-postgresql

docker_volume_recreate: docker_volume_rm docker_volume_create

docker_build_tools: source
	rm docker_build_tools.log || true
	docker buildx build --build-arg JOBS=$(BAG3D_TOOLS_DOCKERIMAGE_JOBS) --build-arg VERSION=$(BAG3D_TOOLS_DOCKERIMAGE_VERSION) --progress plain -t "$(BAG3D_TOOLS_DOCKERIMAGE):$(BAG3D_TOOLS_DOCKERIMAGE_VERSION)" -f "$(BAG3D_TOOLS_DOCKERFILE)" . >> docker_build_tools.log 2>&1

docker_up_postgres:
	docker compose -p bag3d-dev -f docker/compose.yaml up -d data-postgresql
	sleep 5

docker_up:
	docker compose -p bag3d-dev -f docker/compose.yaml up -d

docker_watch:
	docker compose -p bag3d-dev -f docker/compose.yaml watch

docker_build:
	docker compose -p bag3d-dev -f docker/compose.yaml build --no-cache

docker_restart: docker_down_rm docker_volume_rm docker_volume_create docker_up

docker_down:
	docker compose -p bag3d-dev down --remove-orphans

docker_down_rm:
	docker compose -p bag3d-dev down --volumes --remove-orphans --rmi local

test: source
	docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/common/tests/ -v
	docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/ -v
	docker compose -p bag3d-dev exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/ -v 
	docker compose -p bag3d-dev exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/ -v

test_slow: source
	docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/common/tests/ -v --run-slow
	docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/ -v --run-slow 
	docker compose -p bag3d-dev exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/ -v --run-slow
	docker compose -p bag3d-dev exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/ -v --run-slow

test_integration: source
	docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/test_integration.py -v -s --run-all
	docker compose -p bag3d-dev exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/test_integration.py -v -s --run-all
	docker compose -p bag3d-dev exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/test_integration.py -v -s --run-all

test_all: source
	docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/common/tests/ -v --run-slow --run-all
	docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/ -v --run-slow  --run-all
	docker compose -p bag3d-dev exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/ -v --run-slow --run-all
	docker compose -p bag3d-dev exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/ -v --run-slow --run-all

local_venv:
	python3.11 -m venv .venv ; . .venv/bin/activate ; pip install -r $(PWD)/requirements-dev.txt

format:
	. .venv/bin/activate ; ruff format $(PWD)/packages ; ruff check $(PWD)/packages	

