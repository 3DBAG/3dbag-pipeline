include .env

.PHONY: download build run stop venvs start_dagster test

source:
	set -a ; . ./.env ; set +a

download: source
	mkdir -p $(BAG3D_TEST_DATA)
	cd $(BAG3D_TEST_DATA) ; curl -O https://data.3dbag.nl/testdata/test_data.zip ; unzip test_data.zip ; rm test_data.zip
	ln -sfr $(BAG3D_TEST_DATA)/reconstruction_data/input/export/3DBAG/export/quadtree.tsv $(BAG3D_TEST_DATA)/reconstruction_data/input/export_uncompressed/3DBAG/export/quadtree.tsv
 
build: source
	docker build -t $(BAG3D_PG_DOCKERIMAGE) $(BAG3D_PG_DOCKERFILE) --build-arg pg_user=$(BAG3D_PG_USER) --build-arg pg_pswd=$(BAG3D_PG_PASSWORD) --build-arg pg_db=$(BAG3D_PG_DATABASE)
run: source
	docker compose -p bag3d --env-file ./.env -f docker/compose.yaml up -d

stop: source
	docker compose -p bag3d --env-file ./.env -f docker/compose.yaml down

venvs: source
	mkdir -p $(BAG3D_VENVS)
	cd $(BAG3D_VENVS) ; python3.11 -m venv venv_floors_estimation ; python3.11 -m venv venv_party_walls ; python3.11 -m venv venv_core ; python3.11 -m venv venv_dagster ; python3.11 -m venv venv_common
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; cd $(PWD)/packages/floors_estimation ; pip install -e .[dev]
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; cd $(PWD)/packages/party_walls ; pip install -e .[dev]
	. $(BAG3D_VENVS)/venv_core/bin/activate ; cd $(PWD)/packages/core ; pip install -e .[dev]
	. $(BAG3D_VENVS)/venv_dagster/bin/activate ; pip install -r $(PWD)/requirements_dagster_webserver.txt
	. $(BAG3D_VENVS)/venv_common/bin/activate ; cd $(PWD)/packages/common ; pip install -e .[dev]
	
start_dagster: source
	set -a ; . ./.env ; set +a; . $(BAG3D_VENVS)/venv_dagster/bin/activate ; cd $(DAGSTER_HOME) ; dagster dev

test: source
	. $(BAG3D_VENVS)/venv_core/bin/activate ; pytest $(PWD)/packages/core/tests/test_assets_ahn.py -v --runslow
	. $(BAG3D_VENVS)/venv_common/bin/activate ; pytest $(PWD)/packages/common/tests/ -v
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; pytest $(PWD)/packages/party_walls/tests -v
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; coverage run -m pytest $(PWD)/packages/floors_estimation/tests -v

coverage: source
	. $(BAG3D_VENVS)/venv_common/bin/activate ; coverage run -m pytest $(PWD)/packages/common/tests/ -v --runslow ; coverage html
	. $(BAG3D_VENVS)/venv_core/bin/activate ; coverage run -m pytest $(PWD)/packages/core/tests/ -v --runslow; coverage html
	. $(BAG3D_VENVS)/venv_party_walls/bin/activate ; coverage run -m pytest $(PWD)/packages/party_walls/tests -v --runslow ; coverage html
	. $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; coverage run -m pytest $(PWD)/packages/floors_estimation/tests -v ; coverage report

integration: source
	. $(BAG3D_VENVS)/venv_core/bin/activate ; pytest $(PWD)/packages/core/tests/test_integration.py -v -s
# . $(BAG3D_VENVS)/venv_floors_estimation/bin/activate ; pytest $(PWD)/packages/floors_estimation/tests/test_integration.py -v -s
# . $(BAG3D_VENVS)/venv_party_walls/bin/activate ; pytest $(PWD)/packages/party_walls/tests/test_integration.py -v -s
