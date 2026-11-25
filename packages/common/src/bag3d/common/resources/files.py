import re
from pathlib import Path
from shutil import rmtree

from dagster import ConfigurableResource, get_dagster_logger
from pydantic import field_validator

logger = get_dagster_logger("resources.file_store")
DEFAULT_TILES_DIR_NAME = "tiles"


class FileStoreResource(ConfigurableResource):
    """Location of the data files that are generated in the pipeline."""

    root_dir: str

    @property
    def path(self) -> Path:
        """Return the root directory as a Path, creating it if it does not exist."""
        p = Path(self.root_dir).resolve()
        if not p.is_dir():
            p.mkdir(parents=True)
            p.chmod(mode=0o777)
            logger.info(f"Created directory {p}")
        return p

    def rm(self, force: bool = False) -> None:
        """Remove the storage directory."""
        p = Path(self.root_dir)
        if force:
            rmtree(str(p))
        else:
            p.rmdir()
        logger.info(f"Deleted directory {p}")

    def create_subdir(self, subdir: str) -> Path:
        """Create and return a subdirectory within the file store directory."""
        new_dir = self.path / subdir
        new_dir.mkdir(exist_ok=True, parents=True)
        return new_dir

    def stage_dir(self, stage: str) -> Path:
        """Return the directory for a pipeline stage, creating it if needed.

        Args:
            stage: Stage name, e.g. "reconstruction", "party_walls",
                   "floors_estimation", "export", "deploy"

        Returns:
            Path object pointing to stages/{stage}/ under root_dir.
        """
        d = self.path / "stages" / stage
        d.mkdir(parents=True, exist_ok=True)
        return d

    def stage_subdir(self, stage: str, *parts: str) -> Path:
        """Return a subdirectory inside a stage, creating it if needed."""
        d = self.stage_dir(stage).joinpath(*parts)
        d.mkdir(parents=True, exist_ok=True)
        return d


class TilesDirResource(ConfigurableResource):
    """Name of the tiles subdirectory inside a Tyler output directory."""

    name: str = DEFAULT_TILES_DIR_NAME

    @field_validator("name")
    @classmethod
    def validate_name(cls, name: str) -> str:
        if name in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9._~-]+", name):
            raise ValueError(
                "Use a tiles directory name with ASCII letters, digits, hyphens, "
                "underscores, periods, or tildes. Choose a name other than '.' or '..'."
            )
        return name
