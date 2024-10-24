include .env

.PHONY: download build build_tools run stop venvs start_dagster test

source:
	set -a ; . ./.env ; set +a

download: source
	rm -rf $(BAG3D_TEST_DATA)
	mkdir -p $(BAG3D_TEST_DATA)
	cd $(BAG3D_TEST_DATA) ; curl -O https://data.3dbag.nl/testdata/pipeline/test_data_v3.zip ; unzip -q test_data_v3.zip ; rm test_data_v3.zip

docker_volume_create:
	docker volume create bag3d-dev-data-pipeline
	docker run -d --name bag3d-dev-temp-container --mount source=bag3d-dev-data-pipeline,target=/data/volume busybox sleep infinity
	docker cp ./tests/test_data/. bag3d-dev-temp-container:/data/volume
	docker rm -f bag3d-dev-temp-container
	docker volume create bag3d-dev-data-postgresql
	docker run -d --name bag3d-dev-temp-container --mount source=bag3d-dev-data-postgresql,target=/data busybox sleep infinity
	docker exec bag3d-dev-temp-container mkdir /data/pgdata /data/pglog
	docker rm -f bag3d-dev-temp-container

docker_volume_rm:
	docker volume rm -f bag3d-dev-data-pipeline
	docker volume rm -f bag3d-dev-data-postgresql

docker_build_tools: source
	rm docker_build_tools.log || true
	docker buildx build --build-arg JOBS=$(BAG3D_TOOLS_DOCKERIMAGE_JOBS) --build-arg VERSION=$(BAG3D_TOOLS_DOCKERIMAGE_VERSION) --progress plain -t "$(BAG3D_TOOLS_DOCKERIMAGE):$(BAG3D_TOOLS_DOCKERIMAGE_VERSION)" -f "$(BAG3D_TOOLS_DOCKERFILE)" . >> docker_build_tools.log 2>&1

docker_up_postgres:
	docker compose -p bag3d-dev -f docker/compose.yaml up -d data-postgresql

docker_up:
	docker compose -p bag3d-dev -f docker/compose.yaml up -d

docker_restart: docker_down_rm docker_volume_rm docker_volume_create docker_up

docker_down:
	docker compose -p bag3d-dev down --remove-orphans

docker_down_rm:
	docker compose -p bag3d-dev down --volumes --remove-orphans --rmi local

venvs: source
	mkdir -p $(BAG3D_VENVS)
	cd $(BAG3D_VENVS) ; python3.11 -m venv venv_floors_estimation ; python3.11 -m venv venv_party_walls ; python3.11 -m venv venv_core ; python3.11 -m venv venv_dagster ; python3.11 -m venv venv_common
	. $(BAG3D_VENVS)/venv_core/bin/activate ; cd $(PWD)/packages/core ; pip install -e .[dev]
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; cd $(PWD)/packages/floors_estimation ; pip install -e .[dev]
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; cd $(PWD)/packages/party_walls ; pip install -e .[dev]
	. $(BAG3D_VENVS)/venv_dagster/bin/activate ; pip install -r $(PWD)/requirements_dagster_webserver.txt
	. $(BAG3D_VENVS)/venv_common/bin/activate ; cd $(PWD)/packages/common ; pip install -e .[dev]
	
start_dagster: source
	set -a ; . ./.env ; set +a; . $(BAG3D_VENVS)/venv_dagster/bin/activate ; cd $(DAGSTER_HOME) ; dagster dev

test: source
	. $(BAG3D_VENVS)/venv_common/bin/activate ; pytest $(PWD)/packages/common/tests/ -v
	. $(BAG3D_VENVS)/venv_core/bin/activate ; pytest $(PWD)/packages/core/tests/ -v
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; pytest $(PWD)/packages/party_walls/tests/ -v
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; pytest $(PWD)/packages/floors_estimation/tests -v

test_slow: source
	. $(BAG3D_VENVS)/venv_common/bin/activate ; pytest $(PWD)/packages/common/tests/ -v --run-slow
	. $(BAG3D_VENVS)/venv_core/bin/activate ; pytest $(PWD)/packages/core/tests/ -v --run-slow
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; pytest $(PWD)/packages/party_walls/tests -v --run-slow
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; pytest $(PWD)/packages/floors_estimation/tests -v --run-slow

test_all: source
	. $(BAG3D_VENVS)/venv_common/bin/activate ; pytest $(PWD)/packages/common/tests/ -v --run-slow --run-all
	. $(BAG3D_VENVS)/venv_core/bin/activate ; pytest $(PWD)/packages/core/tests/ -v --run-slow --run-all
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; pytest $(PWD)/packages/party_walls/tests -v --run-slow --run-all
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; pytest $(PWD)/packages/floors_estimation/tests -v --run-slow --run-all

integration: source
	. $(BAG3D_VENVS)/venv_core/bin/activate ; pytest $(PWD)/packages/core/tests/test_integration.py -v -s --run-all
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; pytest $(PWD)/packages/party_walls/tests/test_integration.py -v -s --run-all
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; pytest $(PWD)/packages/floors_estimation/tests/test_integration.py -v -s --run-all

docker_integration:
	docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/test_integration.py -v -s --run-all
	docker compose -p bag3d-dev exec bag3d-party-walls pytest /opt/3dbag-pipeline/packages/party_walls/tests/test_integration.py -v -s --run-all
	docker compose -p bag3d-dev exec bag3d-floors-estimation pytest /opt/3dbag-pipeline/packages/floors_estimation/tests/test_integration.py -v -s --run-all

coverage: source
	. $(BAG3D_VENVS)/venv_common/bin/activate ; coverage run -m pytest $(PWD)/packages/common/tests/ -v --run-slow; coverage report
	. $(BAG3D_VENVS)/venv_core/bin/activate ; coverage run -m pytest $(PWD)/packages/core/tests/ -v --run-slow --run-all; coverage report
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; coverage run -m pytest $(PWD)/packages/party_walls/tests/ -v --run-slow --run-all; coverage report
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; coverage run -m pytest $(PWD)/packages/floors_estimation/tests/ -v --run-slow --run-all; coverage report

format: source
	. $(BAG3D_VENVS)/venv_common/bin/activate ; ruff format $(PWD)/packages

