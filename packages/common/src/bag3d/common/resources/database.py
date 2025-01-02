from typing import Optional

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

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        port: Optional[str] = None,
        other_params: Optional[Permissive()] = None,
    ):
        super().__init__(
            host=host or "data-postgresql",
            user=user or "baseregisters_test_user",
            password=password or "baseregisters_test_pswd",
            dbname=dbname or "baseregisters_test",
            port=port or "5432",
            other_params=other_params or {"sslmode": "allow"},
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
