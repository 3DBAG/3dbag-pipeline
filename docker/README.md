# Docker

The sample docker compose file (`docker/compose.yaml`) contains a multi-container setup for running the 3dbag-pipeline in a dockerized environment.
The setup contains:

- multiple dagster services (prefix `dagster_`), 
- one service for each workflow package (prefix `bag3d_`), 
- a database for pipeline generated data (prefix `data_`),
- external, named volumes that store the data for the pipeline,
- a network.

The tests that are run on GitHub Actions use this configuration.

## Docker images

All Docker image versions and external tool-image digests are declared in
[`3dbag-manifest.json`](../3dbag-manifest.json). The Compose Make targets resolve
their image references from that file; image-tag overrides are not supported.

[`3dgi/3dbag-pipeline-tools`](https://hub.docker.com/r/3dgi/3dbag-pipeline-tools)

The 3dbag-pipeline calls several tools in a subprocess, e.g. roofer, tyler, gdal, pdal etc.
We maintain a builder image in `docker/tools/Dockerfile` with all of these tools installed,
and use it as a base image for building the images of the workflow packages (`core`, `floors_estimation`, `party_walls`).

If you need to add a new tool to be used in the pipeline you can one of the following : 

1. If there is a image available for the tool you can make sure it is used when building the `3dbag-pipeline-tools` image by making the necessary modifications in the `docker/tools/Dockerfile` (as it is done for example for tyler)
2. If no image is available, you should update the `tools-build.sh` and `tools-test.sh` files which are used when building the `3dbag-pipeline-tools` image. You should also modify the command in `docker/tools/Dockerfile` to ensure the new tools are installed.

To update a Docker-delivered tool, edit its version, repository, and digest in the
manifest. If the tools image changes, also update `images.tools.version`; CI publishes
that exact tag. Pipeline release images are all tagged with the top-level manifest version.


[`3dgi/3dbag-pipeline-core`](https://hub.docker.com/r/3dgi/3dbag-pipeline-core) 

Contains the `core` package, based on `3dgi/3dbag-pipeline-tools`. 
The image contains all build dependencies for installing the python project, so that it is possible to develop the code in a container.
The Dockerfile is `docker/pipeline/bag3d-core.dockerfile`.

[`3dgi/3dbag-pipeline-floors-estimation`](https://hub.docker.com/r/3dgi/3dbag-pipeline-floors-estimation) 

Contains the `floors_estimation` package, based on `3dgi/3dbag-pipeline-tools`. 
The image contains all build dependencies for installing the python project, so that it is possible to develop the code in a container.
The Dockerfile is `docker/pipeline/bag3d-floors-estimation.dockerfile`.

[`3dgi/3dbag-pipeline-party-walls`](https://hub.docker.com/r/3dgi/3dbag-pipeline-party-walls)

Contains the `party_walls` package, based on `3dgi/3dbag-pipeline-tools`. 
The image contains all build dependencies for installing the python project, so that it is possible to develop the code in a container.
The Dockerfile is `docker/pipeline/bag3d-party-walls.dockerfile`.

[`3dgi/3dbag-pipeline-dagster`](https://hub.docker.com/r/3dgi/3dbag-pipeline-dagster)

Image for running the dagster webserver and daemon.
The Dockerfile is `docker/dagster/Dockerfile`.

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
