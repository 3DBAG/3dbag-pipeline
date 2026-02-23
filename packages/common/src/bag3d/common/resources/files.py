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

    @staticmethod
    def mkdir_temp(temp_dir_id: Optional[str] = None) -> Path:
        """Create a temporary directory with the required permissions.

        Creates a directory at ``/tmp/tmp_3dbag_{temp_dir_id}`` with 777
        permissions so that Docker containers can read and write to it.

        Args:
            temp_dir_id: Identifier for the directory name. If None, generates
                a random 8-character alphabetic string.

        Returns:
            Path object pointing to the created directory.
        """
        if temp_dir_id is None:
            temp_dir_id = "".join(random.choice(string.ascii_letters) for _ in range(8))
        tmp = Path(f"/tmp/tmp_3dbag_{temp_dir_id}")
        tmp.mkdir(exist_ok=True)
        tmp.chmod(mode=0o777)
        return tmp

    @property
    def bag3d_dir(self) -> Path:
        """The 3D BAG data directory"""
        return self.data_dir / "3DBAG"

    @property
    def geoflow_crop_dir(self) -> Path:
        """Directory for the Geoflow crop-reconstruct output"""
        return self.bag3d_dir / "crop_reconstruct"

    def bag3d_export_dir(self, version: str) -> Path:
        """Create the 3DBAG export directory if does not exist"""
        export_dir = self.bag3d_dir / f"export_{version}"
        export_dir.mkdir(exist_ok=True, parents=True)
        return export_dir

    def ahn_dir(self, ahn_version: int) -> Path:
        """Return a directory path where to store the AHN LAZ files for the given AHN
        version."""
        return self.data_dir / "pointcloud" / f"AHN{ahn_version}"

    def ahn_laz_dir(self, ahn_version: int) -> Path:
        """Return a directory path where to store the AHN LAZ files for the given AHN
        version."""
        return self.ahn_dir(ahn_version) / "as_downloaded" / "LAZ"


class FileStoreResource(ConfigurableResource):
    """Location of the data files that are generated in the pipeline.
    data_dir: The directory where the files are stored.
    If None, the resource is initialized with a temporary directory.

    TODO: make the directory functions in .core (bag3d_export_dir etc) members of this
    """

    data_dir: str

    @property
    def file_store(self) -> FileStore:
        return FileStore(data_dir=self.data_dir)
