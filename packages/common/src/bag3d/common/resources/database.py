from typing import Any

from dagster import ConfigurableResource
from pgutils import PostgresConnection

DatabaseConnection = PostgresConnection


class DatabaseResource(ConfigurableResource):
    """
    Database connection resource for PostgreSQL.

    Args:
        host: Database host address
        user: Database username
        password: Database password (optional)
        dbname: Database name
        port: Database port number
        other_params: Additional connection parameters (optional)
    """

    host: str
    user: str
    password: str | None = None
    dbname: str
    port: int
    other_params: dict[str, Any] | None = None

    @property
    def connection(self) -> DatabaseConnection:
        return DatabaseConnection(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            **(self.other_params or {}),
        )
