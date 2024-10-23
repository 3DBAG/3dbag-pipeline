from typing import Optional

from dagster import ConfigurableResource, Permissive

from pgutils import PostgresConnection, PostgresFunctions

DatabaseConnection = PostgresConnection
import pydantic


class DatabaseResource(ConfigurableResource):
    """
    Database connection.
    """

    host: str
    user: str
    password: str
    dbname: str
    port: str
    other_params: pydantic.SkipValidation(Permissive())

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        dbname: str,
        port: str,
        other_params: Optional[pydantic.SkipValidation(Permissive())] = None,
    ):
        super().__init__(
            host=host,
            user=user,
            password=password,
            dbname=dbname,
            port=port,
            other_params=other_params or {},
        )
        conn = DatabaseConnection(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            **self.other_params,
        )
        # Create the utility Postgres functions
        PostgresFunctions(conn)

    @property
    def connect(self):
        conn = DatabaseConnection(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            **self.other_params,
        )
        return conn
