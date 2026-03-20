from dagster import ConfigurableResource


class NlTransform(ConfigurableResource):
    """Transformation properties for CityJSON.
    Used when we need a single transform that applies for the whole Netherlands.
    """

    translate: list[float] = [171800.0, 472700.0, 0.0]
    scale: list[float] = [0.001, 0.001, 0.001]
