from pathlib import Path
from shutil import rmtree

from dagster import get_dagster_logger, ConfigurableResource

logger = get_dagster_logger("resources.file_store")

# Path constants
BAG3D_DIR = "3DBAG"
CROP_RECONSTRUCT_DIR = "3DBAG/crop_reconstruct"
POINTCLOUD_DIR = "pointcloud"
LAZ_SUBDIR = "as_downloaded/LAZ"


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
        """Create and return a subdirectory within the main file store directory.

        Args:
            subdir: Relative path of the subdirectory to create.

        Returns:
            Path object pointing to the created subdirectory.

        Note:
            Creates parent directories if they don't exist.
        """
        new_dir = self.path / subdir
        new_dir.mkdir(exist_ok=True, parents=True)
        return new_dir

    @property
    def bag3d_dir(self) -> Path:
        """Get the main 3D BAG data directory."""
        return self.create_subdir(BAG3D_DIR)

    @property
    def geoflow_crop_dir(self) -> Path:
        """Get the directory for Geoflow crop-reconstruct operation output."""
        return self.create_subdir(CROP_RECONSTRUCT_DIR)

    def bag3d_export_dir(self, version: str) -> Path:
        """Get the 3DBAG export directory for a specific version."""
        return self.create_subdir(f"{BAG3D_DIR}/export_{version}")

    def ahn_laz_dir(self, ahn_version: int) -> Path:
        """Get the directory for AHN LAZ files per version."""
        return self.create_subdir(f"{POINTCLOUD_DIR}/AHN{ahn_version}/{LAZ_SUBDIR}")
