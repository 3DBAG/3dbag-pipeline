from abc import ABC
from time import sleep

from dagster import resource, Field, Shape, Permissive, Noneable, get_dagster_logger
import docker
from docker.errors import ImageNotFound, NotFound
from pgutils import PostgresConnection, PostgresFunctions

from bag3d_pipeline.core import get_run_id


class DockerHub:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password


@resource(
    config_schema={
        "username": Field(
            Noneable(str), description="DockerHub username."
        ),
        "password": Field(
            Noneable(str), description="DockerHub password."
        )
    }
)
def docker_hub(context):
    """DockerHub credentials."""
    return DockerHub(username=context.resource_config["username"],
                     password=context.resource_config["password"])


def make_container_id(run_id):
    return f"3dbag-{run_id}"


class DockerContainer:
    def __init__(self, container_id=None):
        self.id = container_id


@resource(
    config_schema={"id": Field(
        str, is_required=False,
        description="Docker container ID. If empty, it will be created as "
                    "`3dbag-<run ID>.`")}
)
def container(context):
    """Docker container ID."""
    run_id = get_run_id(context, short=True)
    context.log.debug(f"container:config: {context.resource_config}\n run_id: {run_id}")
    cid = context.resource_config.get("id")
    _id = cid if cid is not None else make_container_id(run_id)
    return DockerContainer(container_id=_id)


DatabaseConnection = PostgresConnection


class DockerDatabaseConnection(DatabaseConnection, ABC):
    def __init__(self, image_id: str, container_id: str,
                 user: str, password: str, host: str,
                 port_host: int, dbname: str, environment: dict = None,
                 volumes=None):
        logger = get_dagster_logger("docker_db_connection")

        environment = dict() if environment is None else environment
        volumes = dict() if volumes is None else volumes

        self.docker_client = docker.from_env()
        try:
            self.docker_image = self.docker_client.images.get(image_id)
        except ImageNotFound:
            self.docker_image = self.docker_client.images.pull(image_id)
        try:
            cont = self.docker_client.containers.get(container_id)
            logger.info(f"Using existing container {container_id}")
        except NotFound:
            cont = self.docker_client.containers.create(
                image=self.docker_image, name=container_id, environment=environment,
                volumes=volumes, ports={"5432/tcp": port_host}, detach=True
            )
            logger.info(f"Created container {container_id}")
        cont.start()
        self.container_id = container_id

        # need to wait for the database to start up in the container
        sleep(7)
        super().__init__(user=user, password=password, host=host,
                         port=port_host, dbname=dbname)
        sleep(2)


@resource(
    config_schema={
        "docker": Field(
            Shape(fields={
                "image_id": Field(
                    str, description="Docker image ID."),
                "environment": Field(
                    Permissive(), is_required=False,
                    description="Environment variables to pass to the container."),
                "volumes": Field(
                    Permissive(), is_required=False,
                    description="Docker volumes to pass to the container. "
                                "See the [containers.run()](https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run) parameters for details."),
            }),
            is_required=False,
            description="Docker container parameters."
        ),
        "port": Field(
            int, description="Database port. If running a docker container, this is "
                             "the port on the docker host to map to port 5432 in the "
                             "container."),
        "host": Field(
            str, default_value="localhost",
            description="Database host to connect to. Set to `localhost` if running a "
                        "docker container."),
        "user": Field(
            str, description="Database username."),
        "password": Field(
            str, description="Database password."),
        "dbname": Field(
            str, description="Database to connect to. It must exist.")
    },
    description="Database connection. If `docker` is set, a container will be started "
                "from 'docker.image_id' that serves the database. The container name "
                "is defined by the 'container' resource.",
    required_resource_keys={"container"}
)
def db_connection(context):
    docker_params = context.resource_config.get("docker")
    if docker_params:
        conn = DockerDatabaseConnection(
            image_id=docker_params["image_id"],
            container_id=context.resources.container.id,
            environment=docker_params.get("environment"),
            volumes=docker_params.get("volumes"),
            port_host=context.resource_config["port"],
            host="localhost",
            user=context.resource_config["user"],
            password=context.resource_config["password"],
            dbname=context.resource_config["dbname"]
        )
    else:
        conn = DatabaseConnection(
            user=context.resource_config["user"],
            password=context.resource_config["password"],
            host=context.resource_config["host"],
            port=context.resource_config["port"],
            dbname=context.resource_config["dbname"]
        )
    # Create the utility Postgres functions
    PostgresFunctions(conn)
    return conn
