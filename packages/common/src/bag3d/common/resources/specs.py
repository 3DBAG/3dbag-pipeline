from dagster import ConfigurableResource
from typing import Dict, Optional, Union, Generator, Tuple
from pydantic import PrivateAttr

from bag3d.specs.core import (
    load_attributes_spec,
    Attribute,
    CityJSONLocation,
    GpkgLocation,
)


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

    def applies_to(
        self,
        data_format: str,
        locations: Union[tuple[CityJSONLocation], tuple[GpkgLocation]],
    ) -> Generator[Tuple[str, Attribute], None, None]:
        """Filter the attributes spec for the specified `level`."""
        allowed_formats = ["cityjson", "gpkg"]
        requested_locations = set(locations)
        if data_format not in allowed_formats:
            raise ValueError(
                f"Unsupported data format: {data_format}. Allowed formats are: {allowed_formats}"
            )
        for a_name, a_spec in self.attributes.items():
            if format_spec := getattr(a_spec.applies_to, data_format):
                if attribute_locations := format_spec["locations"]:
                    if len(requested_locations.intersection(attribute_locations)) > 0:
                        yield a_name, a_spec
