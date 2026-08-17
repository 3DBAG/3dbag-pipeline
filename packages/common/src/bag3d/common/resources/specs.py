from collections.abc import Generator

from bag3d.specs.core import (
    Attribute,
    CityJSONLocation,
    GpkgLocation,
    Ogc3dTilesLocation,
    load_attributes_spec,
)
from dagster import ConfigurableResource
from pydantic import PrivateAttr


class Specs3DBAGResource(ConfigurableResource):
    """
    The 3DBAG specifications.

    Source: https://github.com/3DBAG/3dbag-specs
    """

    _attributes_specs: dict[str, Attribute] | None = PrivateAttr(default=None)

    @property
    def attributes(self) -> dict[str, Attribute]:
        """Returns the complete attributes specification."""
        # Lazy load the attributes
        if self._attributes_specs is None:
            self._attributes_specs = load_attributes_spec()
        return self._attributes_specs

    def applies_to(
        self,
        data_format: str,
        locations: tuple[CityJSONLocation]
        | tuple[GpkgLocation]
        | tuple[Ogc3dTilesLocation],
    ) -> Generator[tuple[str, Attribute], None, None]:
        """Filter the attributes spec for the specified data format and location.

        Args:
            data_format: The data format that contains the attribute (`cityjson`, `gpkg`, `ogc3dtiles`).
            locations: The tuple of locations for the attribute in the data format.

        Returns:
            A generator that only contains the attributes that are only in the requested data format and location.
        """
        allowed_formats = ["cityjson", "gpkg", "ogc3dtiles"]
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
