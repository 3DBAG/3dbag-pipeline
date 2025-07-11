from dagster import ConfigurableResource
import urllib.request
import json


class Schema3DBAGResource(ConfigurableResource):
    """
    The 3DBAG attributes schema.

    Source: https://github.com/3DBAG/3dbag-schema
    """

    attributes: dict

    def __init__(
        self,
    ):
        with urllib.request.urlopen(
            "https://raw.githubusercontent.com/3DBAG/3dbag-schema/refs/heads/master/attributes.json"
        ) as url:
            data = json.load(url)
        super().__init__(attributes=data)
