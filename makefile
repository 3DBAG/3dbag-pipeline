include .env

.PHONY: download build run stop venvs start_dagster test

source:
	set -a ; . ./.env ; set +a

download: source
# TODO: Change this to download the baseregisters.tar file from a link
	mkdir -p $(PATH_TO_TEST_DATA)
	rsync -azhP --ignore-existing $(SERVER_NAME):/data/3DBAG_Pipeline_test_data/ $(PATH_TO_TEST_DATA)
	ln -sfr $(PATH_TO_TEST_DATA)/reconstruction_data/input/export/3DBAG/export/quadtree.tsv $(PATH_TO_TEST_DATA)/reconstruction_data/input/export_uncompressed/3DBAG/export/quadtree.tsv
# ln -fsnr $(PATH_TO_TEST_DATA)/reconstruction_data/input/ $(REPO)/packages/party_walls/tests/data
 
build: source
	docker build -t $(IMAGE_NAME) $(PATH_TO_DOCKERFILE) --build-arg pg_user=$(POSTGRES_USER) --build-arg pg_pswd=$(POSTGRES_PASSWORD) --build-arg pg_db=$(POSTGRES_DB) 
run:
	docker compose --env-file ./.env -f docker/compose.yaml up -d

stop: source
	docker container stop $(CONTAINER_NAME)
	docker container rm $(CONTAINER_NAME)

venvs: source
	cd $(PATH_TO_VENVS) ; python3.11 -m venv venv_floors_estimation ; python3.11 -m venv venv_party_walls ; python3.11 -m venv venv_core ; python3.11 -m venv venv_dagster
	. $(PATH_TO_VENVS)/venv_floors_estimation/bin/activate ; cd $(REPO)/packages/floors_estimation ; pip install -e .[dev]
	. $(PATH_TO_VENVS)/venv_party_walls/bin/activate ; cd $(REPO)/packages/party_walls ; pip install -e .[dev]
	. $(PATH_TO_VENVS)/venv_core/bin/activate ; cd $(REPO)/packages/core ; pip install -e .[dev]
	. $(PATH_TO_VENVS)/venv_dagster/bin/activate ; pip install dagster dagster-webserver dagster-postgres
	
start_dagster: source
	cd $(DAGSTER_HOME) ; source $(PATH_TO_VENVS)/venv_dagster/bin/activate ;  dagster dev

test: source
	. $(PATH_TO_VENVS)/venv_core/bin/activate ; pytest $(REPO)/packages/core/tests/ -v ; pytest $(REPO)/packages/common/tests/ -v
# source $(PATH_TO_VENVS)/venv_party_walls/bin/activate ; pytest $(REPO)/packages/party_walls/tests -v
# source $(PATH_TO_VENVS)/venv_floors_estimation/bin/activate ; pytest $(REPO)/packages/floors_estimation/tests -v
