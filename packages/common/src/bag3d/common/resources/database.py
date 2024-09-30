from dagster import ConfigurableResource

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

    @property
    def connection(self):
        conn = DatabaseConnection(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            dbname=self.dbname,
        )
        # Create the utility Postgres functions
        PostgresFunctions(conn)
        return conn
