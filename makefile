include .env

.PHONY: download build run stop venvs start_dagster test

source:
	set -a ; . ./.env ; set +a

download: source
	mkdir -p $(PATH_TO_TEST_DATA)
	cd $(PATH_TO_TEST_DATA) ; curl -O https://data.3dbag.nl/testdata/test_data.zip ; unzip test_data.zip
	ln -sfr $(PATH_TO_TEST_DATA)/reconstruction_data/input/export/3DBAG/export/quadtree.tsv $(PATH_TO_TEST_DATA)/reconstruction_data/input/export_uncompressed/3DBAG/export/quadtree.tsv
 
build: source
	docker build -t $(IMAGE_NAME) $(PATH_TO_DOCKERFILE) --build-arg pg_user=$(POSTGRES_USER) --build-arg pg_pswd=$(POSTGRES_PASSWORD) --build-arg pg_db=$(POSTGRES_DB) 
run: source
	docker compose --env-file ./.env -f docker/compose.yaml up -d

stop: source
	docker container stop $(CONTAINER_NAME)
	docker container rm $(CONTAINER_NAME)

venvs: source
	cd $(PATH_TO_VENVS) ; python3.11 -m venv venv_floors_estimation ; python3.11 -m venv venv_party_walls ; python3.11 -m venv venv_core ; python3.11 -m venv venv_dagster ; python3.11 -m venv venv_common
	. $(PATH_TO_VENVS)/venv_floors_estimation/bin/activate ; cd $(PWD)/packages/floors_estimation ; pip install -e .[dev]
	. $(PATH_TO_VENVS)/venv_party_walls/bin/activate ; cd $(PWD)/packages/party_walls ; pip install -e .[dev]
	. $(PATH_TO_VENVS)/venv_core/bin/activate ; cd $(PWD)/packages/core ; pip install -e .[dev]
	. $(PATH_TO_VENVS)/venv_dagster/bin/activate ; pip install -r $(PWD)/requirements_dagster_webserver.txt
	. $(PATH_TO_VENVS)/venv_common/bin/activate ; cd $(PWD)/packages/common ; pip install -e .[dev]
	
start_dagster: source
	set -a ; . ./.env ; set +a; . $(PATH_TO_VENVS)/venv_dagster/bin/activate ; cd $(DAGSTER_HOME) ; dagster dev

test: source
	. $(PATH_TO_VENVS)/venv_core/bin/activate ; pytest $(PWD)/packages/core/tests/ -v; pytest $(PWD)/packages/common/tests/ -v
	. $(PATH_TO_VENVS)/venv_party_walls/bin/activate ; pytest $(PWD)/packages/party_walls/tests -v
	. $(PATH_TO_VENVS)/venv_floors_estimation/bin/activate ; coverage run -m pytest $(PWD)/packages/floors_estimation/tests -v

coverage: source
	. $(PATH_TO_VENVS)/venv_common/bin/activate ; coverage run -m pytest $(PWD)/packages/common/tests/ -v --runslow ; coverage html
	. $(PATH_TO_VENVS)/venv_core/bin/activate ; coverage run -m pytest $(PWD)/packages/core/tests/ -v --runslow; coverage html
	. $(PATH_TO_VENVS)/venv_party_walls/bin/activate ; coverage run -m pytest $(PWD)/packages/party_walls/tests -v --runslow ; coverage html
	. $(PATH_TO_VENVS)/venv_floors_estimation/bin/activate ; coverage run -m pytest $(PWD)/packages/floors_estimation/tests -v ; coverage report
