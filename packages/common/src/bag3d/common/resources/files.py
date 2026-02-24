from pathlib import Path
from shutil import rmtree
import random
import string
from typing import Optional

from dagster import get_dagster_logger, ConfigurableResource

logger = get_dagster_logger("resources.file_store")


class FileStoreResource(ConfigurableResource):
    """Location of the data files that are generated in the pipeline."""

    data_dir: str

    @property
    def path(self) -> Path:
        """Return the data directory as a Path, creating it if it does not exist."""
        p = Path(self.data_dir).resolve()
        if not p.is_dir():
            p.mkdir()
            p.chmod(mode=0o777)
            logger.info(f"Created directory {p}")
        return p

    def rm(self, force: bool = False) -> None:
        """Remove the storage directory.

        Args:
            force: If True, recursively removes the directory with its contents.
                   If False, only removes an empty directory.

        Warning:
            This permanently deletes data. Use force=True with caution.
        """
        p = Path(self.data_dir)
        if force:
            rmtree(str(p))
        else:
            p.rmdir()
        logger.info(f"Deleted directory {p}")
    
    def create_subdir(self, subdir: str) -> Path:
        """Return a subdirectory within the 3D BAG data directory."""
        new_dir = self.path / subdir
        new_dir.mkdir(exist_ok=True, parents=True)
        return new_dir

    @property
    def bag3d_dir(self) -> Path:
        """The 3D BAG data directory"""
        return self.create_subdir("3DBAG")

    @property
    def geoflow_crop_dir(self) -> Path:
        """Directory for the Geoflow crop-reconstruct output"""
        return self.create_subdir("3DBAG/crop_reconstruct")

    def bag3d_export_dir(self, version: str) -> Path:
        """Create the 3DBAG export directory if does not exist"""
        return self.create_subdir(f"3DBAG/export_{version}")

    def ahn_dir(self, ahn_version: int) -> Path:
        """Return a directory path where to store the AHN LAZ files for the given AHN
        version."""
        return self.create_subdir(f"pointcloud/AHN{ahn_version}")

    def ahn_laz_dir(self, ahn_version: int) -> Path:
        """Return a directory path where to store the AHN LAZ files for the given AHN
        version."""
        return self.create_subdir(f"pointcloud/AHN{ahn_version}/as_downloaded/LAZ")



