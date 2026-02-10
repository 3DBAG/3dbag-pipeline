from typing import Optional, Mapping

from dagster import ConfigurableResource, Permissive

from pgutils import PostgresConnection, PostgresFunctions

DatabaseConnection = PostgresConnection


class DatabaseResource(ConfigurableResource):
    """
    Database connection.
    """

    host: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    dbname: Optional[str] = None
    port: Optional[str] = None
    other_params: Optional[Permissive()] = None

    @property
    def connect(self) -> DatabaseConnection:
        return DatabaseConnection(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            **self.other_params,
        )
