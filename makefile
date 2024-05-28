IMAGE_NAME = ginas_image_postgis
CONTAINER_NAME = ginas_container_postgis
REPO=/Users/localadmin/Repos/3dbag-pipeline
PATH_TO_VENVS = $(REPO)/venvs
PATH_TO_DATA = $(REPO)/gina/data
PATH_TO_DOCKERFILE = $(REPO)/gina/docker/postgres
DAGSTER_HOME = $(REPO)/tests/dagster_home

.PHONY: build run venvs download start

download:
# Change this to download the baseregisters.tar file from a link
	rsync -azhP --ignore-existing gstavropoulou@gilfoyle:baseregisters.tar $(PATH_TO_DATA)

build:
	docker build -t $(IMAGE_NAME) $(PATH_TO_DOCKERFILE) --build-arg version=1.0

run:
	docker run --platform linux/amd64 --rm --name $(CONTAINER_NAME)  -p 5432:5432  -v ${PATH_TO_DATA}:/var/lib/postgresql/data  $(IMAGE_NAME)

stop:
	docker container stop $(CONTAINER_NAME)

venvs:
	cd $(PATH_TO_VENVS) ; python3.11 -m venv venv_floors_estimation ; python3.11 -m venv venv_party_walls ; python3.11 -m venv venv_core ; python3.11 -m venv venv_dagster
	source $(PATH_TO_VENVS)/venv_floors_estimation/bin/activate ; cd $(REPO)/packages/floors_estimation ; pip install .
	source $(PATH_TO_VENVS)/venv_party_walls/bin/activate ; cd $(REPO)/packages/party_walls ; pip install .
	source $(PATH_TO_VENVS)/venv_core/bin/activate ; cd $(REPO)/packages/core ; pip install .
	source $(PATH_TO_VENVS)/venv_dagster/bin/activate ; pip install dagster dagster-webserver dagster-postgres
	
start:
	cd $(DAGSTER_HOME) ; source $(PATH_TO_VENVS)/venv_dagster/bin/activate ;  dagster dev
