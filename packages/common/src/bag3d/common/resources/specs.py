from dagster import ConfigurableResource
from typing import Dict

from bag3d.specs.core import load_attributes_spec, Attribute, AttributeAppliesTo


class Specs3DBAGResource(ConfigurableResource):
    """
    The 3DBAG attributes specifications.

    Attributes:
        feature_attributes: Attributes that apply to a whole feature
        surface_attributes: Attributes that apply to semantic surfaces

    Source: https://github.com/3DBAG/3dbag-specs
    """

    feature_attributes: Dict[str, Attribute]
    surface_attributes: Dict[str, Attribute]

    def __init__(self, **data):
        super().__init__(**data)
        attributes = load_attributes_spec()

        self.cityobject_attributes={
            a_name: a_spec
            for a_name, a_spec in attributes.items()
            if a_spec.applies_to == AttributeAppliesTo.Building
        }
        self.surface_attributes={
            a_name: a_spec
            for a_name, a_spec in attributes.items()
            if a_spec.applies_to != AttributeAppliesTo.Building
        }
