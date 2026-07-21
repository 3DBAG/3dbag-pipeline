"""Resources for the floors estimation workflow."""

from dagster import ConfigurableResource


class ModelStoreResource(ConfigurableResource):
    """A resource for the floors estimation model file path."""

    model_path: str
