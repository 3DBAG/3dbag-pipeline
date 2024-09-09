from dagster import resource, Field, Permissive

from pgutils import PostgresConnection, PostgresFunctions


DatabaseConnection = PostgresConnection


@resource(
    config_schema={
        "port": Field(int, description="Database port."),
        "host": Field(
            str, default_value="localhost", description="Database host to connect to."
        ),
        "user": Field(str, description="Database username."),
        "password": Field(str, description="Database password."),
        "dbname": Field(str, description="Database to connect to. It must exist."),
        "other_params": Permissive(
            description="Other connection parameters to be passed on to the database."
        ),
    },
    description="Database connection.",
)
def db_connection(context):
    conn = DatabaseConnection(
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        host=context.resource_config["host"],
        port=context.resource_config["port"],
        dbname=context.resource_config["dbname"],
        **context.resource_config["other_params"],
    )
    # Create the utility Postgres functions
    PostgresFunctions(conn)
    return conn
