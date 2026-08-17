from dagster import ConfigurableResource
from pydantic import Field


class NlTransform(ConfigurableResource):
    """Transformation properties for CityJSON.
    Used when we need a single transform that applies for the whole Netherlands.
    """

    translate: list[float] = Field(default_factory=lambda: [171800.0, 472700.0, 0.0])
    scale: list[float] = Field(default_factory=lambda: [0.001, 0.001, 0.001])
