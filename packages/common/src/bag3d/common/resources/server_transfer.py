from typing import Optional

from dagster import ConfigurableResource

from fabric import Connection


class ServerTransferResource(ConfigurableResource):
    """
    A resource for transferring files to other servers.
    """

    host: Optional[str] = None
    user: Optional[str] = None
    target_dir: Optional[str] = None

    @property
    def connect(self):
        conn = Connection(host=self.host, user=self.user)
        return conn
