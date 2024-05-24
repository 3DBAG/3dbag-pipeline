IMAGE_NAME = ginas_image_postgis
CONTAINER_NAME = ginas_container_postgis
PATH_TO_DATA = /Users/localadmin/Repos/3dbag-pipeline/gina/data
REPO=/Users/localadmin/Repos/3dbag-pipeline

.PHONY: build run venvs download

download:
	rsync -azhP gstavropoulou@gilfoyle:baseregisters.tar $(REPO)/gina/data/

build:
	docker build -t $(IMAGE_NAME) gina/docker/postgres/ --build-arg version=1.0

run:
	docker run -d --platform linux/amd64 --rm --name $(CONTAINER_NAME)  -p 5432:5432  -v ${PATH_TO_DATA}:/var/lib/postgresql/data  $(IMAGE_NAME)

venvs:
	cd $(REPO)/venvs
	python3.11 -m venv venv_floors_estimation
	source $(REPO)/venvs/venv_floors_estimation/bin/activate
	cd $(REPO)/packages/floors_estimation
	pip install .
	cd $(REPO)/venvs
	python3.11 -m venv venv_party_walls
	source $(REPO)/venvs/venv_party_walls/bin/activate
	cd $(REPO)/packages/party_walls
	pip install .
	cd $(REPO)/venvs
	python3.11 -m venv venv_core
	source $(REPO)/venvs/venv_core/bin/activate
	cd $(REPO)/packages/core
	pip install .
	cd $(REPO)/venvs
	python3.11 -m venv venv_dagster
	source $(REPO)/venvs/venv_dagster/bin/activate
	pip install dagster dagster-webserver dagster-postgres
	cd $(REPO)/tests/dagster_home
	dagster dev