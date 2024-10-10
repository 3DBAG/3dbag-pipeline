from typing import Optional

from dagster import ConfigurableResource, Permissive

from pgutils import PostgresConnection, PostgresFunctions

DatabaseConnection = PostgresConnection


class DatabaseResource(ConfigurableResource):
    """
    Database connection.
    """

    host: str
    user: str
    password: str
    dbname: str
    port: str
    other_params: Permissive()

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        dbname: str,
        port: str,
        other_params: Optional[Permissive()] = None,
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
        try:
            # Create the utility Postgres functions
            PostgresFunctions(conn)
        finally:
            conn.close()

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
