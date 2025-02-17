from pathlib import Path
from typing import Union
from shutil import rmtree
import random
import string

from dagster import get_dagster_logger, ConfigurableResource
import docker
from docker.errors import NotFound
from typing import Optional

logger = get_dagster_logger("resources.file_store")


def make_temp_path(run_id):
    return f"/tmp/tmp_3dbag_{run_id}"


class FileStore:
    # TODO: should have a unified interface regardless if we use a volume or local dir
    def __init__(
        self,
        data_dir: Union[str, Path, None] = None,
        docker_volume_id: Union[str, None] = None,
        dir_id: Union[str, None] = None,
    ):
        self.data_dir = None
        self.docker_volume = None
        if data_dir:
            directory = Path(data_dir)
            p = directory.resolve()
            if p.is_dir():
                pass
                # # Need r+w for others, so that docker containers can write to the
                # # directory
                # if oct(p.stat().st_mode) != "0o40777":
                #     raise PermissionError(f"Need mode=777 on {p}, because docker "
                #                           f"containers need read+write+execute on it.")
            else:
                p.mkdir()
                p.chmod(mode=0o777)
                logger.info(f"Created directory {p}")
            self.data_dir = p
        elif docker_volume_id:
            docker_client = docker.from_env()
            self.docker_volume = None
            try:
                self.docker_volume = docker_client.volumes.get(docker_volume_id)
                logger.info(f"Using existing docker volume: {docker_volume_id}")
            except NotFound:
                self.docker_volume = docker_client.volumes.create(
                    name=docker_volume_id, driver="local"
                )
                logger.info(f"Created docker volume: {docker_volume_id}")
        else:
            # In case dir_id is also None, we create a temp dir with a random ID.
            tmp = self.mkdir_temp(dir_id)
            self.data_dir = tmp
            logger.info(f"Created local temporary directory {self.data_dir}")

    def rm(self, force=False):
        if self.data_dir:
            if force:
                rmtree(str(self.data_dir))
            else:
                self.data_dir.rmdir()
            logger.info(f"Deleted directory {self.data_dir}")
            self.data_dir = None
        if self.docker_volume:
            self.docker_volume.remove(force=force)
            logger.info(f"Deleted docker volume {self.docker_volume}")
            self.docker_volume = None

    @staticmethod
    def mkdir_temp(temp_dir_id: str = None) -> Path:
        """Create a temporary directory with the required permissions.

        The path of the new directory is `/tmp/tmp_3dbag_<temp_dir_id>`.

        Args:
            temp_dir_id (str): The ID-part of the directory name. E.g. the first 8
                characters of the dagster run ID. If None, a random ID is generated.
        """
        if temp_dir_id:
            dir_id = temp_dir_id
        else:
            dir_id = "".join(random.choice(string.ascii_letters) for _ in range(8))
        tmp = Path(make_temp_path(dir_id))
        tmp.mkdir(exist_ok=True)
        tmp.chmod(mode=0o777)
        return tmp


class FileStoreResource(ConfigurableResource):
    """Location of the data files that are generated in the pipeline.
    data_dir: The directory where the files are stored.
    If None, the resource is initialized with a temporary directory.

    TODO: make the directory functions in .core (bag3d_export_dir etc) members of this
    """

    data_dir: Optional[str] = None

    def __init__(
        self,
        data_dir: Optional[Union[Path, str]] = None,
    ):
        super().__init__(data_dir=str(data_dir) if data_dir else None)

    @property
    def file_store(self) -> FileStore:
        if self.data_dir:
            return FileStore(data_dir=self.data_dir)
        else:
            return FileStore()
