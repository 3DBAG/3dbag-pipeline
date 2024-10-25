# Running the 3dbag-pipeline in docker containers

The sample docker compose file (`docker/compose.yaml`) contains a multi-container setup for running the 3dbag-pipeline in a dockerized environment.
The setup contains:

- multiple dagster services (prefix `dagster_`), 
- one service for each workflow package (prefix `bag3d_`), 
- a database for pipeline generated data (prefix `data_`),
- external, named volumes that store the data for the pipeline,
- a network.

The tests that are run on GitHub Actions use this configuration.

## How to run the services

To run the services in `docker/compose.yaml`, do the following steps.

Download the test data files.

```shell
make download
```

Create the docker volumes and copy the test data file into the volumes.

```shell
make docker_volume_create
```

Build the docker images and start the services.

```shell
make docker_up
```

To only start the postgres database with the test data, run:

```shell
make docker_up_postgres
```

Finally, remove the volumes, containers and images created in previous steps.

```shell
make docker_down_rm
```

Rebuild the images, volumes and restart the services.

```shell
make docker_restart
```

## Docker image with the external tools

The 3dbag-pipeline calls several tools in a subprocess, e.g. roofer, tyler, gdal, pdal etc.
We maintain a builder image in `docker/tools/Dockerfile` with all of these tools installed, and use it as a base image for building the images of the workflow packages (`core`, `floors_estimation`, `party_walls`.)

## Local development in docker containers

It is possible to set up your own development environment by compiling all the tools (see `tools-build.sh`) and installing the 3dbag-pipeline packages into virtual environments.

The alternative is to develop the 3dbag-pipeline code in docker containers that already contain the necessary dependencies and tools.

The docker documentation describes in detail how [does the compose watch functionality work](https://docs.docker.com/compose/how-tos/file-watch/).