from dagster import ConfigurableResource
from typing import Dict

from bag3d.specs.core import load_attributes_spec, Attribute


class Specs3DBAGResource(ConfigurableResource):
    """
    The 3DBAG attributes specifications.

    Source: https://github.com/3DBAG/3dbag-specs
    """

    attributes: Dict[str, Attribute]

    def __init__(
        self,
    ):
        super().__init__(attributes=load_attributes_spec())
