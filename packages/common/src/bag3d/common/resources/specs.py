from collections.abc import Mapping
from functools import cached_property

from bag3d.specs.projections.cityjson import (
    CityJSONAttribute,
    CityJSONProjection,
    load_cityjson_projection,
)
from bag3d.specs.projections.geopackage import (
    GeoPackageContainer,
    GeoPackageProjection,
    load_geopackage_projection,
)
from bag3d.specs.projections.ogc3dtiles import (
    Ogc3DTilesProjection,
    Tileset,
    load_ogc3dtiles_projection,
)
from bag3d.specs.semantic import Attribute, load_attributes
from dagster import ConfigurableResource


class Specs3DBAGResource(ConfigurableResource):
    """Semantic attributes and native distribution projections, loaded once."""

    @cached_property
    def attributes(self) -> Mapping[str, Attribute]:
        return load_attributes()

    @cached_property
    def cityjson(self) -> CityJSONProjection:
        return load_cityjson_projection()

    @cached_property
    def geopackage(self) -> GeoPackageProjection:
        return load_geopackage_projection()

    @cached_property
    def ogc3dtiles(self) -> Ogc3DTilesProjection:
        return load_ogc3dtiles_projection()

    def cityjson_attributes(
        self, object_type: str, *, surface: bool = False
    ) -> Mapping[str, CityJSONAttribute]:
        containers = (
            self.cityjson.semantic_surfaces if surface else self.cityjson.objects
        )
        if object_type not in containers:
            raise ValueError(f"Unprojected CityJSON container {object_type!r}")
        return containers[object_type]

    def geopackage_container(self, name: str) -> GeoPackageContainer | None:
        return next((c for c in self.geopackage.containers if c.name == name), None)

    def tileset(self, lod: str) -> Tileset:
        matches = [t for t in self.ogc3dtiles.tilesets if t.lod == lod]
        if len(matches) != 1:
            raise ValueError(f"Expected one projected tileset for {lod!r}")
        return matches[0]
