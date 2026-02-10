from typing import Optional

from dagster import ConfigurableResource, Permissive

from pgutils import PostgresConnection

DatabaseConnection = PostgresConnection


class DatabaseResource(ConfigurableResource):
    """
    Database connection.
    """

    host: str
    user: str
    password: Optional[str] = None
    dbname: str
    port: int
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
