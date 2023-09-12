from time import sleep
from pathlib import Path
import string
from random import choice
from shutil import rmtree
import os

import pytest
import docker
from docker import errors

from bag3d.common.resources.database import (DatabaseConnection, db_connection,
                                             container)

RES_CONTAINER_ID = "pytest-3dbag-pipeline-db_connection"


@pytest.fixture(scope="session", autouse=True)
def setenv():
    os.environ["DAGSTER_DEPLOYMENT"] = "pytest"


@pytest.fixture(scope="session", autouse=False)
def database():
    """Database for testing, including test data. Runs in a docker container."""
    image_name = "balazsdukai/3dbag-sample-data:latest"
    container_name = "pytest-3dbag-pipeline"
    port = 5565

    # temp dir that is mounted on the container for data
    r = ''.join(choice(string.ascii_letters) for _ in range(8))
    tmp = Path(f"/tmp/pytest-3dbag-{r}")
    tmp.mkdir()
    tmp.chmod(mode=0o777)

    # set up
    client = docker.from_env()
    try:
        image = client.images.get(image_name)
    except errors.ImageNotFound:
        image = client.images.pull(image_name)
    try:
        cont = client.containers.run(
            image, name=container_name, detach=True, volumes=[f"{tmp}:/data", ],
            ports={"5432/tcp": port})
    except errors.DockerException as e:
        try:
            cont = client.containers.get(container_name)
        except errors.ContextNotFound:
            pytest.fail(f"Could not start docker container from {image_name}:\n{e}")
        else:
            cont.remove(force=True)
            pytest.fail(f"Could not start docker container from {image_name}:\n{e}")

    # check setup
    print(f"Mounted {image_name}/data onto {tmp}")
    print(f"{tmp} contents:")
    for i in tmp.iterdir():
        print(i)

    # run tests, but wait for database to start first
    sleep(8)
    db = DatabaseConnection(user="db3dbag_user", password="db3dbag_1234",
                            host="localhost", port=port, dbname="baseregisters")
    yield tmp, db

    # teardown
    cont.remove(force=True, v=True)
    rmtree(str(tmp))
    print(f"Deleted docker container {container_name}")
    print(f"Deleted local directory {tmp}")


@pytest.fixture(scope="session")
def resource_container():
    return container.configured({"id": RES_CONTAINER_ID})


@pytest.fixture(scope="function")
def resource_db_connection_docker(resource_container):
    yield db_connection.configured({
        "docker": {
            "image_id": "balazsdukai/3dbag-sample-data:latest",
        },
        "port": 5564,
        "user": "db3dbag_user",
        "password": "db3dbag_1234",
        "dbname": "baseregisters"
    })
    client = docker.from_env()
    cont = client.containers.get(RES_CONTAINER_ID)
    cont.remove(force=True, v=True)


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="function")
def wkt_testarea():
    """A small test area in the oldtown of Utrecht, incl. the Oudegracht."""
    yield "Polygon ((136251.531 456118.126, 136620.128 456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 456118.126))"


@pytest.fixture(scope="session")
def docker_gdal_image():
    """The GDAL docker image to use for the tests"""
    return "osgeo/gdal:alpine-small-latest"
