from typing import Optional

from dagster import ConfigurableResource
import random
import string


class VersionResource(ConfigurableResource):
    """
    A resource for setting up the version release.
    """

    version: Optional[str] = None

    def __init__(
        self,
        version: Optional[str] = None,
    ):
        super().__init__(
            version=version
            or "".join(random.choice(string.ascii_letters) for _ in range(8))
        )
