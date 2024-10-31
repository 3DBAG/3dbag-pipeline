# Development guide

Thank you for considering to contribute to the 3dbag-pipeline.
In this document we describe how can you get set up for developing the software locally, how to submit a contribution, what guidelines do we follow and what do we expect from your contribution, what can you expect from us.

## Setup

After cloning the repository from [https://github.com/3DBAG/3dbag-pipeline](https://github.com/3DBAG/3dbag-pipeline), the recommended way to set up your environment is with docker.

### For contributing to the documentation

The documentation is built with [mkdocs](https://www.mkdocs.org/) and several plugins.
First, install the documentation dependencies (into a virtual environment).

```shell
pip install -r requirements_docs.txt
```

Start the *mkdocs* server.

```shell
mkdocs serve
```

Verify that you can see the local documentation on `http://127.0.0.1:8000/` in your browser.

### For contributing to the code

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
![](images/docker_compose_run_config.png)

Start the services by running the configuration from the compose file.
For example, see the screenshot below. 
![](images/docker_compose_start.png)

Set up the python interpreter in the docker container as the project interpreter, using PyCharm's docker-compose interpreter setup.
Note here that you need to use the matching service for the 3dbag-pipeline package. 
For example, for working on the `core` package, you need to configure the `bag3d-core` service for the python interpreter.

To run a specific test, set up a run configuration with the python interpreter in docker and make sure to use the environment variables from the `docker/.env` file.
![](images/docker_compose_test_config.png)

For further details, see the [PyCharm documentation](https://www.jetbrains.com/help/pycharm/using-docker-compose-as-a-remote-interpreter.html#run).

## Documenting the package

The APIs (eg. `common`) is documented with [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).
In order to generate the API documentation for a package, the package must be installed.
Solely for documentation purposes, this is best done with `pip install --no-deps packages/<package>`.


