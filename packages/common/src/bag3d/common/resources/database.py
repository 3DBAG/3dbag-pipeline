from abc import ABC
from time import sleep

from dagster import resource, Field, Permissive

from pgutils import PostgresConnection, PostgresFunctions


DatabaseConnection = PostgresConnection
@resource(
    config_schema={
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
            str, description="Database to connect to. It must exist."),
        "other_params": Permissive(
            description="Other connection parameters to be passed on to the database."
        )
    },
    description="Database connection. If `docker` is set, a container will be started "
                "from 'docker.image_id' that serves the database. The container name "
                "is defined by the 'container' resource."
)
def db_connection(context):
    conn = DatabaseConnection(
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        host=context.resource_config["host"],
        port=context.resource_config["port"],
        dbname=context.resource_config["dbname"],
        **context.resource_config["other_params"]
        )
    # Create the utility Postgres functions
    PostgresFunctions(conn)
    return conn
