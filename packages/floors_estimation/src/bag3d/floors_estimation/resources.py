"""Resources for the floors estimation workflow."""

from dagster import ConfigurableResource


class ModelStoreResource(ConfigurableResource):
    """A resource for the floors estimation model file path."""

    model_path: str


class TrainingDataResource(ConfigurableResource):
    """A resource for the directory containing training data files."""

    data_dir: str
