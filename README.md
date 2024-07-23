# 3D BAG pipeline

Repository of the 3D BAG production pipeline.

## Quickstart for local development:

Requires:

- Python 3.11
- SSH connection to gilfoyle
- Docker

First you need to set up the following environment variables in a `.env` file in root of this repository. The `.env` file is required for running the makefile.

```shell
# contents of .env 
REPO=<Path to repo>
PATH_TO_VENVS=${REPO}/venvs
PATH_TO_TEST_DATA=${REPO}/test_data
PATH_TO_DOCKERFILE=${REPO}/docker/postgres
DAGSTER_HOME=${REPO}/tests/dagster_home

IMAGE_NAME=bag3d_image_postgis
CONTAINER_NAME=bag3d_container_postgis

POSTGRES_USER=baseregisters_test_user
POSTGRES_PASSWORD=baseregisters_test_pswd
POSTGRES_DB=baseregisters_test
POSTGRES_PORT=5560

SERVER_NAME=<net_id>@gilfoyle
SERVER_RECONSTRUCTION_DIR=/fastssd/data/3DBAG/crop_reconstruct_og
SERVER_3DBAG_DIR=/data/3DBAG/

BAG3D_EXPORT_DIR=${PATH_TO_TEST_DATA}/reconstruction_data/input/export/3DBAG/export
```

Then you run the tests from the root directory of the repo with:

```shell
make venvs
make download
make build 
make run
make test
```

Where:
make venvs = creates the vitrual environments
make download = downloads test_data from the server
make build = building the postgres image
make run = starts the postgres container
make test =  runs the tests for core package. 

## Packages

The packages are organized into a `common` package and a number of workflow packages.
The `common` package contains the resources, functions and type definitions that are used by the 3D BAG packages that define the data processing workflows.
The workflow packages contain the assets, jobs, sensors etc. that define a data processing workflow for a part of the complete 3D BAG.

The reason for this package organisation is that workflow packages have widely different dependencies, and installing them into the same environment bound to lead to dependency conflicts.
Additionally, this oranisation makes it easier to install and test the workflow packages in isolation.

- [`common`](/packages/common/README.md): The common package used by the workflow packages.
- [`core`](/packages/core/README.md): Workflow for producing the core of the 3D BAG data set.
- [`party_walls`](/packages/party_walls/README.md): Workflow for calculating the party walls.
- [`floors-estimation`](/packages/floors_estimation/README.md): Workflow for estimating the number of floors.

## Documentation

The `common` package contains API documentation that can be viewed locally.
The documentation of the components of the workflow packages can be viewed in the Dagster UI.

## Development and testing

You need to have the workflow packages set up in their own virtual environments in `/venvs`.
The virtual environment names follow the pattern of `venv_<package>`. You need to set up:  `/venvs/venv_core`, `/venvs/venv_party_walls` and `/venvs/venv_floors_estimation`

The dagster UI (dagster-webserver) is installed and run separately from the *bag3d* packages, as done in our deployment setup. Create another virtual environment for the `dagster-webserver` and install the package with `pip install dagster-webserver`.

The `DAGSTER_HOME` contains the configuration for loading the *bag3d* packages into the main dagster instance, which we can operate via the UI. 
In order to launch a local development dagster instance, navigate to the local `DAGSTER_HOME` (see below) and start the development instance.
If you've set up the virtual environment correctly, this main dagster instance will load the *code location* of each workflow package.

```shell
cd 3dbag-pipeline/tests/dagster_home
dagster dev
```

The UI is served at `http://localhost:3000`, but check the logs in the terminal for the details.

To set up all this in one step you can run :

```bash
make venvs
```

### Data

Dependencies:
- [just](https://just.systems/)
- docker

*Just* reads some variables from a dotenv (`.env`) file from the root `3dbag-pipeline` directory.
These variables define the download server and the location of the data.
Thus, you need to create a `.env` file and set these variables:

```shell
# contents of .env
SERVER_NAME="server URL that will be used by rsync"
SERVER_3DBAG_DIR="path to the 3D BAG data directory on the server"
SERVER_RECONSTRUCTION_DIR="path to the crop_reconstruct dir"
```

The test data setup across all packages is managed with just from the root directory.
You need to init the data directories and download all test files:

```shell
just download
```

** Note: ** On a mac, you might be getting an error `ln: illegal option -- r`. You need to install :

```bash
brew search coreutils 
export PATH="/opt/homebrew/opt/coreutils/libexec/gnubin:$PATH"
```
to be able to use te `-r` flag.

The downloaded files are placed into `3dbag-pipeline/tests/data` and symlinked to each package so that *pytest* can find them easily.

Finally, you can clean up with `just clean`.

### Conventions

SQL files are stored in the `sqlfiles` subpackage, so that the `bag3d.common.utils.database.load_sql` function can load them.

The dependency graph of the 3D BAG packages is strictly `common`<--*workflow packages*, thus workflow packages cannot depend on each other.
If you find that you need to depend on functionality in another workflow package, move that function to `common`.

Docstrings follow the [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). 
However, *dagster* is too smart for it's own good and if you describe the return value with the `Returns:` section, then *dagster* will only display the text of the `Returns:` section in the dagster UI.
A workaround for this is to include the `Returns:` heading in the return value description.
For example `Returns a collection type, storing the...`

Assets are usually some results of computations, therefore their names are nouns, not verbs.

### Unit testing

Tests are run separately for each package and they are located in the `tests` directory of the package.
Tests use `pytest` and `tox`.

The tests use the sample data from the `3dbag-sample-data` docker image.
A container is created from this image and bind-mounted on a new temporary directory, which is populated with the sample data from the image.
The tests then either use the data from this temporary directory or from the database in the container.
See the `tests/conftest.py` on how this is set up.

#### Long running tests

Some test take a long time to execute. 
If you mark them with the `@pytest.mark.slow` decorator, they will be skipped by default.
In order to include the slow tests in the test execution, use the `--runslow` command line option.

```shell
pytest --runslow
```

### Dagster

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

## Source datasets

They are downloaded with the `source_input` job and they are:

- [BAG](docs/SOURCE_DATASETS.md#bag)
- [AHN](docs/SOURCE_DATASETS.md#ahn)
- [BGT](docs/SOURCE_DATASETS.md#bgt)
- [TOP10NL](docs/SOURCE_DATASETS.md#top10nl)

Read more about [the source datasets here](docs/SOURCE_DATASETS.md).


## Deployment

The deployment configurations are managed with the `DAGSTER_DEPLOYMENT` environment variable.
Possible values are in `bag3d.common.resources`.

Sometimes the dagster instance storage schema changes and the schema needs to be updated with `dagster instance migrate`.


## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
