from typing import Optional

from dagster import ConfigurableResource

from fabric import Connection


class ServerTransferResource(ConfigurableResource):
    """
    A resource for transferring files to other servers.

    Attributes:
        host: Optional[str]
            The hostname or IP address of the remote server.
        port: Optional[int]
            The port to connect to on the remote server.
        user: Optional[str]
            The username to use for authentication.
        password: Optional[str]
            The password to use for authentication (if not using key).
        key_filename: Optional[str]
            The path to the private key file for key-based authentication.
        target_dir: Optional[str]
            The default target directory on the remote server for file transfers.
        public_dir: Optional[str]
            The 3DBAG public directory on the remote server.
    """

    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    key_filename: Optional[str] = None
    target_dir: Optional[str] = None
    public_dir: Optional[str] = None

    @property
    def connection(self):
        connect_kwargs = {}
        if self.key_filename:
            connect_kwargs["key_filename"] = self.key_filename
        elif self.password:
            connect_kwargs["password"] = self.password
        return Connection(
            host=self.host,
            port=self.port,
            user=self.user,
            connect_kwargs=connect_kwargs,
        )

    def transfer_file(self, local_path, remote_path):
        """Transfer a file to remote server."""
        with self.connection as conn:
            # Upload the file
            conn.put(local_path, remote_path)

            # Verify the file was uploaded
            result = conn.run(f"ls -la {remote_path}", hide=True)
            return result.ok

    def file_exists(self, remote_path):
        """Check if file exists on remote server."""
        with self.connection as conn:
            result = conn.run(f"test -f {remote_path}", warn=True, hide=True)
            return result.ok
