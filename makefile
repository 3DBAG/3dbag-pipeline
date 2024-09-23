include .env

.PHONY: download build run stop venvs start_dagster test

source:
	set -a ; . ./.env ; set +a

download: source
	rm -rf $(BAG3D_TEST_DATA)
	mkdir -p $(BAG3D_TEST_DATA)
	cd $(BAG3D_TEST_DATA) ; curl -O https://data.3dbag.nl/testdata/test_data_v2.zip ; unzip test_data_v2.zip ; rm test_data_v2.zip
 
build: source
	docker build -t $(BAG3D_PG_DOCKERIMAGE) $(BAG3D_PG_DOCKERFILE) --build-arg pg_user=$(BAG3D_PG_USER) --build-arg pg_pswd=$(BAG3D_PG_PASSWORD) --build-arg pg_db=$(BAG3D_PG_DATABASE)
run: source
	docker compose -p bag3d --env-file ./.env -f docker/compose.yaml up -d

stop: source
	docker compose -p bag3d --env-file ./.env -f docker/compose.yaml down

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

integration: source
	. $(BAG3D_VENVS)/venv_core/bin/activate ; pytest $(PWD)/packages/core/tests/test_integration.py -v -s --run-all
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; pytest $(PWD)/packages/party_walls/tests/test_integration.py -v -s --run-all
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; pytest $(PWD)/packages/floors_estimation/tests/test_integration.py -v -s --run-all

coverage: source
	. $(BAG3D_VENVS)/venv_common/bin/activate ; coverage run -m pytest $(PWD)/packages/common/tests/ -v --run-slow; coverage report
	. $(BAG3D_VENVS)/venv_core/bin/activate ; coverage run -m pytest $(PWD)/packages/core/tests/ -v --run-slow --run-all; coverage report
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; coverage run -m pytest $(PWD)/packages/party_walls/tests/ -v --run-slow --run-all; coverage report
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; coverage run -m pytest $(PWD)/packages/floors_estimation/tests/ -v --run-slow --run-all; coverage report

format: source
	. $(BAG3D_VENVS)/venv_common/bin/activate ; ruff format $(PWD)/packages

