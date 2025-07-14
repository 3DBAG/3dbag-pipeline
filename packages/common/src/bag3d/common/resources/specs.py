from dagster import ConfigurableResource
import urllib.request
import json


class Specs3DBAGResource(ConfigurableResource):
    """
    The 3DBAG attributes specifications.

    Source: https://github.com/3DBAG/3dbag-specs
    """

    attributes: dict

    def __init__(
        self,
    ):
        with urllib.request.urlopen(
            "https://raw.githubusercontent.com/3DBAG/3dbag-specs/refs/heads/master/attributes.json"
        ) as url:
            data = json.load(url)
        super().__init__(attributes=data)
