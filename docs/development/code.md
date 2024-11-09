# Code

Thank you for considering to contribute to the 3dbag-pipeline.
In this document we describe how can you get set up for developing the software locally, how to submit a contribution, what guidelines do we follow and what do we expect from your contribution, what can you expect from us.

## Setup

After cloning the repository from [https://github.com/3DBAG/3dbag-pipeline](https://github.com/3DBAG/3dbag-pipeline), the recommended way to set up your environment is with docker.

Requirements:
- make
- docker engine
- docker compose (>= 2.22.0)

We use `make` for managing many commands that we use in development.

#### Test data

Download test data.

```bash
export BAG3D_TEST_DATA=${PWD}/tests/test_data
make download
```

Create the docker volumes that store the test data.

```shell
make docker_volume_create
```

In addition, `make docker_volume_rm` removes the volumes, `make docker_volume_recreate` recreates the volumes.

Note that if you change the test data locally and you want that the docker services use the updated data, then you need to,
1. stop the services: `make docker_down`
2. recreate the volumes in order to copy the new data into them: `make docker_volume_recreate`
3. start the service again: `make docker_up`

#### Docker setup

Start the docker containers with `watch` enabled.
If you issue the command the first time, several things will happen:

1. the required base images are pulled from DockerHub,
2. the 3dbag-pipeline workflow images are built from the local source code,
3. the containers are connected to the volumes and networks,
4. the dagster-webserver is published on `localhost:3003`,
5. docker compose will start watching changes in the source code on the host machine.

```shell
make docker_watch
```

The running containers contain all the tools that are required for a complete run of the 3dbag-pipeline.
This means that you can develop and test any part of the code locally.

If you make a change in the source code in your code editor, the files are synced automatically into the running containers, so you can see your changes in effect **after reloading the code location, job, asset or resource** in the dagster UI on `localhost:3003`.

The docker documentation describes in detail how [does the compose watch functionality work](https://docs.docker.com/compose/how-tos/file-watch/).

If you don't want to enable the code synchronization with `watch`, the `make docker_up` command starts the containers in normally.

The `docker_watch`, `docker_up` targets will set the docker compose project name to `bag3d-dev`.

#### Docker setup in PyCharm (professional)

Create run configuration that uses the docker compose file.
For example, see the screenshot below. 
![](../images/docker_compose_run_config.png)

Start the services by running the configuration from the compose file.
For example, see the screenshot below. 
![](../images/docker_compose_start.png)

Set up the python interpreter in the docker container as the project interpreter, using PyCharm's docker-compose interpreter setup.
Note here that you need to use the matching service for the 3dbag-pipeline package. 
For example, for working on the `core` package, you need to configure the `bag3d-core` service for the python interpreter.

To run a specific test, set up a run configuration with the python interpreter in docker and make sure to use the environment variables from the `docker/.env` file.
![](../images/docker_compose_test_config.png)

For further details, see the [PyCharm documentation](https://www.jetbrains.com/help/pycharm/using-docker-compose-as-a-remote-interpreter.html#run).

#### Local setup

You need to have the workflow packages set up in their own virtual environments in `/venvs`.
The virtual environment names follow the pattern of `venv_<package>`. You need to set up:  `/venvs/venv_core`, `/venvs/venv_party_walls` and `/venvs/venv_floors_estimation`

The dagster UI (dagster-webserver) is installed and run separately from the *bag3d* packages, as done in our deployment setup.
Create another virtual environment for the `dagster-webserver` and install the required packages from `requirements_dagster_webserver.txt`.

```bash
pip install -r requirements_dagster_webserver.txt
```

To set up all this in one step you can run (make sure you've set the .env variables):

```bash
make venvs
```

The `DAGSTER_HOME` contains the configuration for loading the *bag3d* packages into the main dagster instance, which we can operate via the UI. 
In order to launch a local development dagster instance, navigate to the local `DAGSTER_HOME` (see below) and start the development instance with:

```bash
cd tests/dagster_home
dagster dev
```

If you've set up the virtual environment correctly, this main dagster instance will load the *code location* of each workflow package.

You can also start it up directly with:

```bash
make start_dagster
```

The UI is served at `http://localhost:3000`, but check the logs in the terminal for the details.

##### Requirements for running the fast tests

- Python 3.11
- Docker

##### Requirements for running the slow and integration tests and for production

- [Tyler](https://github.com/3DGI/tyler)
- [Geoflow-roofer](https://github.com/3DBAG/geoflow-roofer)
- [LAStools](https://github.com/LAStools/LAStools)
- [gdal](https://github.com/OSGeo/gdal)
- [pdal](https://github.com/PDAL/PDAL)

The `build-tools.sh` Bash script can help you to build the required tools. 
See `build-tools.sh --help` for usage instructions.
Note that you need to run `build-tools.sh` with `bash` (not `sh`), and it can take a 
couple of hours to build everything.
Requirements for building the tools:

- C and C++ compilers (min. GCC 13 or Clang 18)
- CMake
- Rust toolchain
- Git
- wget
- libgeos
- sqlite3
- libtiff

#### Environment variables

First you need to set up the following environment variables in a `.env` file in root of this repository. The `.env` file is required for running the commands in the makefile. For just running the fast tests, the following variables are necessary and no modification is needed (the tests will run in a docker-based database):

```bash
BAG3D_VENVS=${PWD}/venvs
BAG3D_TEST_DATA=${PWD}/tests/test_data
BAG3D_FLOORS_ESTIMATION_MODEL=${BAG3D_TEST_DATA}/model/pipeline_model1_gbr_untuned.joblib
BAG3D_EXPORT_DIR=${BAG3D_TEST_DATA}/reconstruction_input/3DBAG/export
BAG3D_RELEASE_VERSION="10_24"

DAGSTER_HOME=${PWD}/tests/dagster_home
TOOLS_DIR=${HOME}/.build-3dbag-pipeline

BAG3D_TOOLS_DOCKERFILE=${PWD}/docker/tools/Dockerfile
BAG3D_TOOLS_DOCKERIMAGE=bag3d_image_tools
BAG3D_TOOLS_DOCKERIMAGE_VERSION=2024.09.24
BAG3D_TOOLS_DOCKERIMAGE_JOBS=8

BAG3D_PG_DOCKERFILE=${PWD}/docker/postgres/Dockerfile
BAG3D_PG_DOCKERIMAGE=bag3d_image_postgis
BAG3D_PG_USER=baseregisters_test_user
BAG3D_PG_PASSWORD=baseregisters_test_pswd
BAG3D_PG_DATABASE=baseregisters_test
BAG3D_PG_HOST=localhost
BAG3D_PG_PORT=5560
BAG3D_PG_SSLMODE=allow

TYLER_RESOURCES_DIR=${TOOLS_DIR}/share/tyler/resources
TYLER_METADATA_JSON=${TOOLS_DIR}/share/tyler/resources/geof/metadata.json

LD_LIBRARY_PATH=${TOOLS_DIR}/lib:$LD_LIBRARY_PATH
PROJ_DATA=${TOOLS_DIR}/share/proj
EXE_PATH_TYLER=${TOOLS_DIR}/bin/tyler
EXE_PATH_TYLER_DB=${TOOLS_DIR}/bin/tyler-db
EXE_PATH_ROOFER_CROP=${TOOLS_DIR}/bin/crop
EXE_PATH_ROOFER_RECONSTRUCT=${TOOLS_DIR}/bin/geof
FLOWCHART_PATH_RECONSTRUCT=${TOOLS_DIR}/share/geoflow-bundle/flowcharts/reconstruct_bag.json
GF_PLUGIN_FOLDER=${TOOLS_DIR}/share/geoflow-bundle/plugins
EXE_PATH_OGR2OGR=${TOOLS_DIR}/bin/ogr2ogr
EXE_PATH_OGRINFO=${TOOLS_DIR}/bin/ogrinfo
EXE_PATH_SOZIP=${TOOLS_DIR}/bin/sozip
EXE_PATH_PDAL=${TOOLS_DIR}/bin/pdal
EXE_PATH_LAS2LAS=${TOOLS_DIR}/bin/las2las64
EXE_PATH_LASINDEX=${TOOLS_DIR}/bin/lasindex64
```

However, for running the integration tests and some of the unit tests you need the [full requirements installation](#requirements-for-running-the-slow-and-integration-tests-and-for-production) and you need to add the paths to your local tools installations to the `.env` file.


You can set up your environment with:

```shell
make venvs
make download
make docker_volume_create
make docker_up_postgres
```

Where:
make venvs = creates the vitrual environments
make download = downloads test_data from the server
make docker_volume_create = create the docker volumes that mount the test data onto the postgres container
make docker_up_postgres = starts the postgres container

Then you can run the fast unit test for all packages with:
 
 ```shell
 make test
 ```

For running also the slow tests (which require more time) you can run:

  ```shell
 make test_slow
 ```

 For running all tests, including the ones that [require building the tools](#requirements-for-running-the-slow-and-integration-tests-and-for-production) and the integration tests, you can run:

 ```shell
 make test_full
 ```

## Conventions

SQL files are stored in the `sqlfiles` subpackage, so that the `bag3d.common.utils.database.load_sql` function can load them.

The dependency graph of the 3D BAG packages is strictly `common`<--*workflow packages*, thus workflow packages cannot depend on each other.
If you find that you need to depend on functionality in another workflow package, move that function to `common`.

Docstrings follow the [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). 
However, *dagster* is too smart for it's own good and if you describe the return value with the `Returns:` section, then *dagster* will only display the text of the `Returns:` section in the dagster UI.
A workaround for this is to include the `Returns:` heading in the return value description.
For example `Returns a collection type, storing the...`

Assets are usually some results of computations, therefore their names are nouns, not verbs.

## Tests

You can run the full tests, including integration with :

```bash
make test_full
```
####  Unit testing

Tests are run separately for each package and they are located in the `tests` directory of the package.
Tests use `pytest`.

The tests use the sample data that are downloaded as shown above.
You can run the unit tests with:

```bash
make test
```

#### Long running tests

Some test take a long time to execute. 
If you mark them with the `@pytest.mark.slow` decorator, they will be skipped by default.
In order to include the slow tests in the test execution, use the `--run-slow` command line option.

```bash
pytest --run-slow
```

These tests require the [full requirements installation](#requirements-for-running-the-slow-and-integration-tests-and-for-production)


#### Integration tests

The integrations tests are made in such way so that the main jobs that comprise the pipeline are run for a small region of 

```bash
make integration
```

## Dagster

#### Terminate all in the queue

Needs to be executed in the environment where the Dagster UI and the Dagster-daemon are running.
This is currently `/opt/dagster/venv` on gilfoyle.
On gilfoyle, need to source all the environment variables first (`/opt/dagster/dagster_home/.env`).

On gilfyole:

```shell
su dagster
export DAGSTER_HOME=/opt/dagster/dagster_home
source DAGSTER_HOME=/opt/dagster/dagster_home/.env
source /opt/dagster/venv/bin/activate
```

```python
from dagster import DagsterInstance, RunsFilter, DagsterRunStatus

instance = DagsterInstance.get() # needs your DAGSTER_HOME to be set, DAGSTER_HOME=/opt/dagster/dagster_home on gilfoyle

while True:
    queued_runs = instance.get_runs(limit=100, filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED]))
    if not queued_runs:
        break
    for run in queued_runs:
        instance.report_run_canceled(run)
```

#### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, start the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process in the same folder as your `workspace.yaml` file, but in a different shell or terminal.

The `$DAGSTER_HOME` environment variable must be set to a directory for the daemon to work. Note: using directories within /tmp may cause issues. See [Dagster Instance default local behavior](https://docs.dagster.io/deployment/dagster-instance#default-local-behavior) for more details.

In this repository the `$DAGSTER_HOME` is in `tests/dagster_home`.

```bash
export DAGSTER_HOME=<absolute path to tests/dagster_home>
dagster-daemon run
```

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

#### GraphQL API

Dagster has a GraphQL API and it is served alongside the dagster-webserver at `/graphql` (eg `http://localhost:3000/graphql`).
One can do basically everything that is doable in the Dagster UI.
Retrieve data on assets, runs etc., but also launch runs.

This query to get the asset materializations metadata and asset dependencies (lineage):

```
{
  assetNodes(
    group: {
      groupName: "top10nl"
      repositoryName: "__repository__"
      repositoryLocationName: "core_py_311_virtual_env"
    }
    pipeline: {
      pipelineName: "source_input"
      repositoryName: "__repository__"
      repositoryLocationName: "core_py_311_virtual_env"
    }
    # assetKeys: { path: ["top10nl", "stage_top10nl_gebouw"] }
    loadMaterializations: true
  ) {
    assetKey {
      path
    }
    dependencies {
      asset {
        assetKey{path}
      }
    }
    assetMaterializations(limit: 1) {
      runId
      assetLineage {
        assetKey {
          path
        }
        partitions
      }
      metadataEntries {
        label
        description
        __typename
        ... on TextMetadataEntry {
          text
        }
        __typename
        ... on IntMetadataEntry {
          intValue
        }
      }
    }
  }
}
```