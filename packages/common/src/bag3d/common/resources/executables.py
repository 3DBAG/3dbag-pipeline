import os
from logging import Logger
from pathlib import Path
from dataclasses import dataclass, field
import signal
from subprocess import PIPE, Popen
from typing import Dict, Optional

from dagster import (
    get_dagster_logger,
    ConfigurableResource,
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


@dataclass(frozen=True)
class DockerContainerConfig:
    """Configuration for running a command in a standalone Docker container.

    Tool containers are expected to mount the same named volume as the pipeline
    containers at the same path (/data/volume), so no path rewriting is needed.

    Example:
        DockerContainerConfig(
            image="ghcr.io/osgeo/gdal:ubuntu-small-latest",
            volumes=["bag3d-dev-data-pipeline:/data/volume:rw"],
            network="bag3d-dev-network",
        )
    """

    image: str
    volumes: list[str] = field(default_factory=list)
    network: str = ""
    environment: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)


class CommandRunner:
    """Unified command execution interface.

    Supports configured executables, raw commands, and Docker containers.
    Works with or without Dagster context.

    When docker_container_cfg is provided, commands run in a standalone Docker
    container. The container mounts the same named volume as the pipeline
    container at the same path, so all file paths in commands are unchanged.
    The Docker client is initialized lazily on first use, making CommandRunner
    safe to pickle for ProcessPoolExecutor.
    """

    def __init__(
        self,
        exes: dict[str, str] = None,
        docker_container_cfg: DockerContainerConfig = None,
    ):
        self.exes = exes or {}
        self._docker_container_cfg = docker_container_cfg
        self._docker_client = None
        self._docker_image = None

    @property
    def with_docker(self) -> bool:
        return self._docker_container_cfg is not None

    def _ensure_docker(self):
        """Lazily connect to Docker and pull/get the image on first use."""
        if self._docker_client is None:
            self._docker_client = docker.from_env()
            try:
                self._docker_image = self._docker_client.images.get(
                    self._docker_container_cfg.image
                )
            except ImageNotFound:
                self._docker_image = self._docker_client.images.pull(
                    self._docker_container_cfg.image
                )

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
            format_dict["local_path"] = local_path

        return command.format(**format_dict)

    def _run_direct(
        self, command: str, cwd: str = None, env: dict = None
    ) -> CommandResult:
        """Execute subprocess without Dagster context."""
        merged_env = {**os.environ, **env} if env else None
        sub_process = Popen(
            command,
            shell=True,
            stdout=PIPE,
            stderr=PIPE,
            cwd=cwd,
            env=merged_env,
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

        merged_env = {**os.environ, **env} if env else None
        sub_process = Popen(
            command,
            shell=True,
            stdout=PIPE,
            stderr=PIPE,
            cwd=cwd,
            env=merged_env,
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

    def _run_docker(
        self,
        command: str,
        logger: Logger = None,
        env: dict = None,
    ) -> CommandResult:
        """Execute command in a standalone Docker container.

        The container mounts the same named volume as the pipeline container,
        joins the same network, and streams logs in real-time to the Dagster
        logger. Returns a CommandResult with stdout/stderr for callers that
        parse tool output.
        """
        self._ensure_docker()
        cfg = self._docker_container_cfg

        # Merge config-level env with per-call env
        container_env = {**cfg.environment, **(env or {})} or None

        # Labels for identification and cleanup
        labels = {"bag3d.managed-by": "3dbag-pipeline", **cfg.labels}

        _logger = logger if logger else get_dagster_logger()
        _logger.info(f"Executing in Docker ({cfg.image}): {command}")

        container = self._docker_client.containers.run(
            self._docker_image,
            command=command,
            volumes=cfg.volumes or None,
            network=cfg.network or None,
            environment=container_env,
            labels=labels,
            detach=True,
            remove=False,
            stdout=True,
            stderr=True,
        )

        # Stream logs in real-time to the Dagster logger
        for log_line in container.logs(stream=True, follow=True):
            line = log_line.decode("utf-8", errors="replace").rstrip("\n")
            if line:
                _logger.info(line)

        result = container.wait()
        exit_code = result.get("StatusCode", 1)

        # Read stdout and stderr separately for CommandResult
        stdout = container.logs(stdout=True, stderr=False).decode("utf-8")
        stderr = container.logs(stdout=False, stderr=True).decode("utf-8")

        if exit_code != 0:
            _logger.error(f"Container exited with code {exit_code}")

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
            command: The command to execute. Can contain {exe} and {local_path} placeholders.
            exe_name: Name of the executable to substitute for {exe}.
            kwargs: Additional keyword arguments for command formatting.
            local_path: Path substituted for {local_path} placeholder.
            cwd: Working directory for subprocess execution (ignored in Docker mode).
            logger: Dagster logger for structured logging.
            env: Environment variables merged into the execution environment.

        Returns:
            CommandResult with returncode, stdout, and stderr.
        """
        final_command = self._build_command(command, exe_name, kwargs, local_path)

        if self.with_docker:
            return self._run_docker(final_command, logger=logger, env=env)
        elif logger is not None:
            return self._run_with_logging(final_command, cwd, env, logger)
        else:
            return self._run_direct(final_command, cwd, env)

    def version(self, exe: str, version_cmd: str = "--version") -> str:
        """Get version of an executable."""
        result = self.run(f"{{exe}} {version_cmd}", exe_name=exe)
        return format_version_stdout(result.stdout)


def _docker_container_cfg_from_env(image: str) -> DockerContainerConfig:
    """Build a DockerContainerConfig using the shared volume and network from env vars."""
    volume_name = os.getenv("BAG3D_DOCKER_VOLUME_DATA_PIPELINE", "")
    network = os.getenv("BAG3D_DOCKER_NETWORK", "")
    volumes = [f"{volume_name}:/data/volume:rw"] if volume_name else []
    return DockerContainerConfig(image=image, volumes=volumes, network=network)


class GDALResource(ConfigurableResource):
    """
    A GDAL Resource can be configured by either the local EXE paths
    for `ogr2ogr`, `ogrinfo` and `sozip`, or by providing a Docker image.

    For the local exes you can use:

        gdal_resource = GDALResource(exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
                                     exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
                                     exe_sozip=os.getenv("EXE_PATH_SOZIP"))

    For the docker image you can use:

        gdal_resource = GDALResource(docker_image=DOCKER_GDAL_IMAGE)

    After the resource has been instantiated, gdal (CommandRunner) can
    be acquired with the `runner` property:

        gdal_resource.runner
    """

    exe_ogrinfo: str = ""
    exe_ogr2ogr: str = ""
    exe_sozip: str = ""
    docker_image: str = ""

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_image:
            return {
                "ogrinfo": "ogrinfo",
                "ogr2ogr": "ogr2ogr",
                "sozip": "sozip",
            }
        return {
            "ogrinfo": self.exe_ogrinfo,
            "ogr2ogr": self.exe_ogr2ogr,
            "sozip": self.exe_sozip,
        }

    @property
    def runner(self) -> CommandRunner:
        if self.docker_image:
            return CommandRunner(
                exes=self.exes,
                docker_container_cfg=_docker_container_cfg_from_env(self.docker_image),
            )
        return CommandRunner(exes=self.exes)


class PDALResource(ConfigurableResource):
    """
    A PDAL Resource can be configured by either the local EXE path
    for `pdal` or by providing a Docker image.

    For the local exe you can use:

        pdal_resource = PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL"))

    For the docker image you can use:

        pdal_resource = PDALResource(docker_image=DOCKER_PDAL_IMAGE)

    After the resource has been instantiated, pdal (CommandRunner) can
    be acquired with the `runner` property:

        pdal_resource.runner
    """

    exe_pdal: str = ""
    docker_image: str = ""

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_image:
            return {"pdal": "pdal"}
        return {"pdal": self.exe_pdal}

    @property
    def runner(self) -> CommandRunner:
        if self.docker_image:
            return CommandRunner(
                exes=self.exes,
                docker_container_cfg=_docker_container_cfg_from_env(self.docker_image),
            )
        return CommandRunner(exes=self.exes)


class LASToolsResource(ConfigurableResource):
    """
    A LASTools Resource can be configured by providing the paths to
    LASTools executables "lasindex" and "las2las" on the local system,
    or by providing a Docker image.

    Example:

        lastools_resource = LASToolsResource(exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
                                             exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
                                             exe_lasinfo=os.getenv("EXE_PATH_LASINFO"))

    After the resource has been instantiated, lastools (CommandRunner) can
    be acquired with the `runner` property:

        lastools_resource.runner
    """

    exe_lasindex: str = ""
    exe_las2las: str = ""
    exe_lasinfo: str = ""
    docker_image: str = ""

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_image:
            return {
                "lasindex": "lasindex",
                "las2las": "las2las",
                "lasinfo": "lasinfo",
            }
        return {
            "lasindex": self.exe_lasindex,
            "las2las": self.exe_las2las,
            "lasinfo": self.exe_lasinfo,
        }

    @property
    def runner(self) -> CommandRunner:
        if self.docker_image:
            return CommandRunner(
                exes=self.exes,
                docker_container_cfg=_docker_container_cfg_from_env(self.docker_image),
            )
        return CommandRunner(exes=self.exes)


class TylerResource(ConfigurableResource):
    """
    A Tyler Resource can be configured by providing the paths to
    Tyler executables "tyler" and "tyler-db" on the local system,
    or by providing a Docker image.

    Example:

        tyler_resource = TylerResource(exe_tyler=os.getenv("EXE_PATH_TYLER"),
                                       exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"))

    After the resource has been instantiated, tyler (CommandRunner) can
    be acquired with the `runner` property:

        tyler = tyler_resource.runner
    """

    exe_tyler: str = ""
    exe_tyler_db: str = ""
    exe_tyler_multiformat: str = ""
    docker_image: str = ""

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_image:
            return {
                "tyler": "tyler",
                "tyler-db": "tyler-db",
                "tyler-multiformat": "tyler-multiformat",
            }
        return {
            "tyler": self.exe_tyler,
            "tyler-db": self.exe_tyler_db,
            "tyler-multiformat": self.exe_tyler_multiformat,
        }

    @property
    def runner(self) -> CommandRunner:
        if self.docker_image:
            return CommandRunner(
                exes=self.exes,
                docker_container_cfg=_docker_container_cfg_from_env(self.docker_image),
            )
        return CommandRunner(exes=self.exes)


class ValidationResource(ConfigurableResource):
    """
    A ValidationResource can be configured by providing the paths to
    the val3dity, cjval and cjio executables on the local system,
    or by providing a Docker image.

    For the local exes you can use:

        validation_resource = ValidationResource(exe_val3dity=os.getenv("EXE_PATH_VAL3DITY"),
                                                 exe_cjval=os.getenv("EXE_PATH_CJVAL"),
                                                 exe_cjio=os.getenv("EXE_PATH_CJIO"))

    After the resource has been instantiated, val3dity (CommandRunner) can
    be acquired with the `runner` property:

        validation = validation_resource.runner
    """

    exe_val3dity: str = ""
    exe_cjval: str = ""
    exe_cjio: str = ""
    docker_image: str = ""

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_image:
            return {
                "val3dity": "val3dity",
                "cjval": "cjval",
                "cjio": "cjio",
            }
        return {
            "val3dity": self.exe_val3dity,
            "cjval": self.exe_cjval,
            "cjio": self.exe_cjio,
        }

    @property
    def runner(self) -> CommandRunner:
        if self.docker_image:
            return CommandRunner(
                exes=self.exes,
                docker_container_cfg=_docker_container_cfg_from_env(self.docker_image),
            )
        return CommandRunner(exes=self.exes)


class RooferResource(ConfigurableResource):
    """
    A RooferResource can be configured by providing the paths to
    Roofer `crop` and `roofer` executables on the local system,
    or by providing a Docker image.

    Example:

        roofer_resource = RooferResource(exe_crop=os.getenv("EXE_PATH_ROOFER_CROP"),
                                         exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"))

    After the resource has been instantiated, roofer (CommandRunner) can
    be acquired with the `runner` property:

        roofer = roofer_resource.runner
    """

    exe_crop: str = ""
    exe_roofer: str = ""
    docker_image: str = ""

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_image:
            return {
                "crop": "crop",
                "roofer": "roofer",
            }
        return {
            "crop": self.exe_crop,
            "roofer": self.exe_roofer,
        }

    @property
    def runner(self) -> CommandRunner:
        if self.docker_image:
            return CommandRunner(
                exes=self.exes,
                docker_container_cfg=_docker_container_cfg_from_env(self.docker_image),
            )
        return CommandRunner(exes=self.exes)


class GeoflowResource(ConfigurableResource):
    """
    A GeoflowResource can be configured by providing the paths to
    Geoflow `exe_geoflow` executable on the local system
    and the path to the reconstruction flowchart,
    or by providing a Docker image.

    Example:

        geoflow_resource = GeoflowResource(exe_geoflow = os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
                                           flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"))

    After the resource has been instantiated, geoflow (CommandRunner) can
    be acquired with the `runner` property:

        geoflow = geoflow_resource.runner
    """

    exe_geoflow: str = ""
    flowchart: str = ""
    docker_image: str = ""

    @property
    def exes(self) -> Dict[str, str]:
        if self.docker_image:
            return {"geof": "geof"}
        return {"geof": self.exe_geoflow}

    @property
    def runner(self) -> CommandRunner:
        if self.docker_image:
            return CommandRunner(
                exes=self.exes,
                docker_container_cfg=_docker_container_cfg_from_env(self.docker_image),
            )
        return CommandRunner(exes=self.exes)
