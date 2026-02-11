import os
from logging import Logger
from pathlib import Path
from dataclasses import dataclass
import signal
from subprocess import PIPE, Popen
from typing import Dict, Optional

from dagster import (
    get_dagster_logger,
    ConfigurableResource,
    Config,
)
import docker
from docker.errors import ImageNotFound

DOCKER_PDAL_IMAGE = "pdal/pdal:sha-cfa827b6"  # PDAL 2.4.3
DOCKER_GDAL_IMAGE = "ghcr.io/osgeo/gdal:ubuntu-small-latest"


@dataclass(frozen=True)
class CommandResult:
    """Immutable result of a command execution."""

    returncode: int
    stdout: str
    stderr: str = ""

    @property
    def success(self) -> bool:
        return self.returncode == 0

    @property
    def has_error_in_output(self) -> bool:
        """Heuristic check - caller decides how to handle."""
        combined = (self.stdout + self.stderr).lower()
        return "error" in combined or "fatal" in combined or "critical" in combined


def format_version_stdout(version: str) -> str:
    return version.replace("\n", ",")


class DockerConfig(Config):
    image: str
    mount_point: str


class CommandRunner:
    """Unified command execution interface.

    Supports configured executables, raw commands, and Docker containers.
    Works with or without Dagster context.
    """

    def __init__(
        self,
        exes: dict[str, str] = None,
        docker_cfg: DockerConfig = None,
        with_docker: bool = False,
    ):
        self.exes = exes or {}
        self.with_docker = with_docker
        if self.with_docker:
            self.docker_client = docker.from_env()
            try:
                self.docker_image = self.docker_client.images.get(docker_cfg.image)
            except ImageNotFound:
                self.docker_image = self.docker_client.images.pull(docker_cfg.image)
            self.container_mount_point = Path(docker_cfg.mount_point)
        else:
            self.docker_client = None
            self.docker_image = None
            self.container_mount_point = None

    @staticmethod
    def _pre_exec():
        """Restore default signal disposition and invoke setsid."""
        for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), signal.SIG_DFL)
        os.setsid()

    def _build_command(
        self,
        command: str,
        exe_name: str = None,
        kwargs: dict = None,
        local_path: Path = None,
    ) -> str:
        """Build final command string with substitutions."""
        format_dict = {}

        if exe_name:
            format_dict["exe"] = self.exes[exe_name]

        if kwargs:
            format_dict.update(kwargs)

        if local_path:
            if self.with_docker:
                if local_path.is_dir():
                    format_dict["local_path"] = self.container_mount_point
                else:
                    format_dict["local_path"] = (
                        self.container_mount_point / local_path.name
                    )
            else:
                format_dict["local_path"] = local_path

        return command.format(**format_dict)

    def _run_direct(
        self, command: str, cwd: str = None, env: dict = None
    ) -> CommandResult:
        """Execute subprocess without Dagster context."""
        sub_process = Popen(
            command,
            shell=True,
            stdout=PIPE,
            stderr=PIPE,
            cwd=cwd,
            env=env,
            preexec_fn=self._pre_exec,
            encoding="UTF-8",
        )
        stdout, stderr = sub_process.communicate()
        return CommandResult(
            returncode=sub_process.returncode,
            stdout=stdout,
            stderr=stderr,
        )

    def _run_with_logging(
        self,
        command: str,
        cwd: str = None,
        env: dict = None,
        logger: Logger = None,
    ) -> CommandResult:
        """Execute subprocess with Dagster logging integration."""
        _logger = logger if logger else get_dagster_logger()
        _logger.info(f"Executing: {command}")

        sub_process = Popen(
            command,
            shell=True,
            stdout=PIPE,
            stderr=PIPE,
            cwd=cwd,
            env=env,
            preexec_fn=self._pre_exec,
            encoding="UTF-8",
        )
        stdout, stderr = sub_process.communicate()

        # Log to Dagster UI for debugging
        if stdout.strip():
            _logger.info(f"stdout:\n{stdout}")
        if stderr.strip():
            _logger.warning(f"stderr:\n{stderr}")
        if sub_process.returncode != 0:
            _logger.error(f"Command failed with exit code {sub_process.returncode}")

        return CommandResult(
            returncode=sub_process.returncode,
            stdout=stdout,
            stderr=stderr,
        )

    def _run_docker(self, command: str, local_path: Path = None) -> CommandResult:
        """Execute in Docker container with proper exit code capture."""
        volumes = None
        if local_path:
            if local_path.is_dir():
                container_path = self.container_mount_point
            else:
                container_path = self.container_mount_point / local_path.name
            volumes = [f"{local_path}:{container_path}"]

        container = self.docker_client.containers.run(
            self.docker_image,
            command=command,
            volumes=volumes,
            network_mode="host",
            detach=True,
            remove=False,
            stdout=True,
            stderr=True,
        )

        result = container.wait()
        exit_code = result.get("StatusCode", 1)
        stdout = container.logs(stdout=True, stderr=False).decode("utf-8")
        stderr = container.logs(stdout=False, stderr=True).decode("utf-8")
        container.remove()

        return CommandResult(
            returncode=exit_code,
            stdout=stdout,
            stderr=stderr,
        )

    def run(
        self,
        command: str,
        *,
        exe_name: str = None,
        kwargs: dict = None,
        local_path: Path = None,
        cwd: str = None,
        logger: Logger = None,
        env: dict = None,
    ) -> CommandResult:
        """Execute command and return structured result.

        Args:
            logger:
            command: The command to execute. Can contain {exe} and {local_path} placeholders.
            exe_name: Name of the executable to substitute for {exe}.
            kwargs: Additional keyword arguments for command formatting.
            local_path: Path to mount in Docker or use in command.
            cwd: Working directory for command execution.
            env: Environment variables for subprocess.

        Returns:
            CommandResult with returncode, stdout, and stderr.
        """
        final_command = self._build_command(command, exe_name, kwargs, local_path)

        if self.with_docker:
            return self._run_docker(final_command, local_path)
        elif logger is not None:
            return self._run_with_logging(final_command, cwd, env, logger)
        else:
            return self._run_direct(final_command, cwd, env)

    def version(self, exe: str, version_cmd: str = "--version") -> str:
        """Get version of an executable."""
        result = self.run(f"{{exe}} {version_cmd}", exe_name=exe)
        return format_version_stdout(result.stdout)


class GDALResource(ConfigurableResource):
    """
    A GDAL Resource can be configured by either the local EXE paths
    for `ogr2ogr`, `ogrinfo` and `sozip`, or by providing the DockerConfig
    for the GDAL image.

    For the local exes you can use:

        gdal_resource = GDALResource(exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
                                     exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
                                     exe_sozip=os.getenv("EXE_PATH_SOZIP"))

    For the docker image you can use:

        gdal_local = GDALResource(docker_cfg=DockerConfig(
                                image=DOCKER_GDAL_IMAGE,
                                mount_point="/tmp"))

    If instantiated with GDALResource() then the Docker image is used by
    default. After the resource has been instantiated, gdal (CommandRunner) can
    be acquired with the `runner` property:

        gdal_resource.runner
    """

    exe_ogrinfo: str
    exe_ogr2ogr: str
    exe_sozip: str
    docker_cfg: Optional[DockerConfig] = None

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_cfg is None:
            return {
                "ogrinfo": self.exe_ogrinfo,
                "ogr2ogr": self.exe_ogr2ogr,
                "sozip": self.exe_sozip,
            }
        else:
            return {
                "ogrinfo": "ogrinfo",
                "ogr2ogr": "ogr2ogr",
                "sozip": "sozip",
            }

    @property
    def with_docker(self) -> bool:
        if (
            self.exe_ogrinfo is None
            and self.exe_ogr2ogr is None
            and self.exe_sozip is None
        ):
            return True
        else:
            return False

    @property
    def runner(self) -> CommandRunner:
        return CommandRunner(
            exes=self.exes, docker_cfg=self.docker_cfg, with_docker=self.with_docker
        )


class PDALResource(ConfigurableResource):
    """
    A PDAL Resource can be configured by either the local EXE path
    for `pdal` or by providing the DockerConfig for the PDAL image.

    For the local exe you can use:

        pdal_resource = PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL"))

    For the docker image you can use:

        pdal_resource = PDALResource(docker_cfg=DockerConfig(
                                        image=DOCKER_PDAL_IMAGE,
                                        mount_point="/tmp"))

    If instantiated with PDALResource() then the Docker image is used by
    default. After the resource has been instantiated, pdal (CommandRunner) can
    be acquired with the `runner` property:

        pdal_resource.runner
    """

    exe_pdal: str
    docker_cfg: Optional[DockerConfig] = None

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_cfg is None:
            return {
                "pdal": self.exe_pdal,
            }
        else:
            return {
                "pdal": "pdal",
            }

    @property
    def with_docker(self) -> bool:
        if self.exe_pdal == "pdal":
            return True
        else:
            return False

    @property
    def runner(self) -> CommandRunner:
        return CommandRunner(
            exes=self.exes, docker_cfg=self.docker_cfg, with_docker=self.with_docker
        )


class LASToolsResource(ConfigurableResource):
    """
    A LASTools Resource can be configured by providing the paths to
    LASTools executables "lasindex" and "las2las" on the local system.

    Example:

        lastools_resource = LASToolsResource(exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
                                             exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
                                             exe_lasinfo=os.getenv("EXE_PATH_LASINFO"))

    After the resource has been instantiated, lastools (CommandRunner) can
    be acquired with the `runner` property:

        lastools_resource.runner
    """

    exe_lasindex: str
    exe_las2las: str
    exe_lasinfo: str

    @property
    def exes(self) -> Dict[str, str]:
        return {
            "lasindex": self.exe_lasindex,
            "las2las": self.exe_las2las,
            "lasinfo": self.exe_lasinfo,
        }

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def runner(self) -> CommandRunner:
        return CommandRunner(exes=self.exes, with_docker=self.with_docker)


class TylerResource(ConfigurableResource):
    """
    A Tyler Resource can be configured by providing the paths to
    Tyler executables "tyler" and "tyler-db" on the local system.

    Example:

        tyler_resource = TylerResource(exe_tyler=os.getenv("EXE_PATH_TYLER"),
                                       exe_tyler_db=s.getenv("EXE_PATH_TYLER_DB"))

    After the resource has been instantiated, tyler (CommandRunner) can
    be acquired with the `runner` property:

        tyler = tyler_resource.runner
    """

    exe_tyler: str
    exe_tyler_db: str
    exe_tyler_multiformat: str

    @property
    def exes(self) -> Dict[str, str]:
        return {
            "tyler": self.exe_tyler,
            "tyler-db": self.exe_tyler_db,
            "tyler-multiformat": self.exe_tyler_multiformat,
        }

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def runner(self) -> CommandRunner:
        return CommandRunner(exes=self.exes, with_docker=self.with_docker)


class ValidationResource(ConfigurableResource):
    """
    A ValidationResource can be configured by providing the paths to
    the val3dity, cjval and cjio executables on the local system.

    For the local exes you can use:

        validation_resource = ValidationResource(exe_val3dity=os.getenv("EXE_PATH_VAL3DITY"),
                                                 exe_cjval=os.getenv("EXE_PATH_CJVAL"),
                                                 exe_cjio=os.getenv("EXE_PATH_CJIO"))

    After the resource has been instantiated, val3dity (CommandRunner) can
    be acquired with the `runner` property:

        validation = validation_resource.runner
    """

    exe_val3dity: str
    exe_cjval: str
    exe_cjio: str

    @property
    def exes(self) -> Dict[str, str]:
        return {
            "val3dity": self.exe_val3dity,
            "cjval": self.exe_cjval,
            "cjio": self.exe_cjio,
        }

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def runner(self) -> CommandRunner:
        return CommandRunner(exes=self.exes, with_docker=self.with_docker)


class RooferResource(ConfigurableResource):
    """
    A RooferResource can be configured by providing the paths to
    Roofer `crop` and `roofer` executables on the local system.

    Example:

        roofer_resource = RooferResource(exe_crop=os.getenv("EXE_PATH_ROOFER_CROP"),
                                         exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"))

    After the resource has been instantiated, roofer (CommandRunner) can
    be acquired with the `runner` property:

        roofer = roofer_resource.runner
    """

    exe_crop: str
    exe_roofer: str

    @property
    def exes(self) -> Dict[str, str]:
        return {
            "crop": self.exe_crop,
            "roofer": self.exe_roofer,
        }

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def runner(self) -> CommandRunner:
        return CommandRunner(exes=self.exes, with_docker=self.with_docker)


class GeoflowResource(ConfigurableResource):
    """
    A GeoflowResource can be configured by providing the paths to
    Geoflow `exe_geoflow` executable on the local system
    and the path to the reconstruction flowchart.

    Example:

        geoflow_resource = GeoflowResource(exe_geoflow = os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
                                           flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"))

    After the resource has been instantiated, geoflow (CommandRunner) can
    be acquired with the `runner` property:

        geoflow = geoflow_resource.runner
    """

    exe_geoflow: str
    flowchart: str

    @property
    def exes(self) -> Dict[str, str]:
        return {"geof": self.exe_geoflow}

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def runner(self) -> CommandRunner:
        return CommandRunner(exes=self.exes, with_docker=self.with_docker)
