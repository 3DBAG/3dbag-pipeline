from dagster import ConfigurableResource, InitResourceContext
from typing import Dict, Optional
from pydantic import PrivateAttr

from bag3d.specs.core import load_attributes_spec, Attribute, AttributeAppliesTo


class Specs3DBAGResource(ConfigurableResource):
    """
    The 3DBAG specifications.

    Source: https://github.com/3DBAG/3dbag-specs
    """

    _attributes_specs: Optional[Dict[str, Attribute]] = PrivateAttr(default=None)

    @property
    def attributes(self) -> Dict[str, Attribute]:
        """Returns the complete attributes specification."""
        # Lazy load the attributes
        if self._attributes_specs is None:
            self._attributes_specs = load_attributes_spec()
        return self._attributes_specs

    def applies_to(self, level: AttributeAppliesTo) -> Dict[str, Attribute]:
        """Filter the attributes spec for the specified `level`."""
        return {
            a_name: a_spec
            for a_name, a_spec in self.attributes.items()
            if a_spec.applies_to == level
        }
