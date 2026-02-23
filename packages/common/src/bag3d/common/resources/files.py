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
