import os
from logging import Logger
from pathlib import Path
from dataclasses import dataclass
import signal
from subprocess import PIPE, Popen
from typing import Dict, ClassVar

from dagster import (
    get_dagster_logger,
    ConfigurableResource,
)
from pydantic import model_validator, Field
import docker
from docker.errors import ImageNotFound


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


class CommandRunner:
    """Unified command execution interface.

    Supports configured executables, raw commands, and Docker containers.
    Works with or without Dagster context.

    When ``with_docker=True`` the runner launches a standalone Docker container
    for each command.  The container joins the network named by the
    ``BAG3D_DOCKER_NETWORK`` environment variable and mounts the named volume
    identified by ``BAG3D_DOCKER_VOLUME_DATA_PIPELINE`` at ``/data/volume``.
    If those variables are not set the container falls back to host networking
    with no extra volume mount.
    """

    def __init__(
        self,
        exes: dict[str, str] = None,
        docker_image: str = "",
        with_docker: bool = False,
    ):
        self.exes = exes or {}
        self.with_docker = with_docker
        self._docker_image_ref = docker_image
        if self.with_docker:
            self.docker_client = docker.from_env()
            try:
                self.docker_image = self.docker_client.images.get(docker_image)
            except ImageNotFound:
                self.docker_image = self.docker_client.images.pull(docker_image)
        else:
            self.docker_client = None
            self.docker_image = None

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
            # In Docker mode, file paths are identical inside and outside the
            # pipeline container because the same named data volume is mounted
            # at /data/volume in all containers.  No remapping is needed.
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

    def _run_docker(
        self,
        command: str,
        env: dict = None,
        logger: Logger = None,
    ) -> CommandResult:
        """Execute in a standalone Docker container.

        The container joins the Docker network identified by the
        ``BAG3D_DOCKER_NETWORK`` environment variable and mounts the named
        volume identified by ``BAG3D_DOCKER_VOLUME_DATA_PIPELINE`` at
        ``/data/volume``.  Falls back to host networking when the variable is
        not set.

        Args:
            command: Shell command to run inside the container.
            env: Extra environment variables to pass to the container.
            logger: Dagster logger; when provided, stdout/stderr are forwarded.
        """
        network = os.getenv("BAG3D_DOCKER_NETWORK")
        volume_name = os.getenv("BAG3D_DOCKER_VOLUME_DATA_PIPELINE")

        run_kwargs: dict = {
            "command": command,
            "detach": True,
            "remove": False,
            "stdout": True,
            "stderr": True,
        }
        if network:
            run_kwargs["network"] = network
        else:
            run_kwargs["network_mode"] = "host"
        if volume_name:
            run_kwargs["volumes"] = {
                volume_name: {"bind": "/data/volume", "mode": "rw"}
            }
        if env:
            run_kwargs["environment"] = env

        if logger:
            logger.info(f"Running in Docker ({self._docker_image_ref}): {command}")

        container = self.docker_client.containers.run(self.docker_image, **run_kwargs)
        result = container.wait()
        exit_code = result.get("StatusCode", 1)
        stdout = container.logs(stdout=True, stderr=False).decode("utf-8")
        stderr = container.logs(stdout=False, stderr=True).decode("utf-8")
        container.remove()

        if logger:
            if stdout.strip():
                logger.info(f"stdout:\n{stdout}")
            if stderr.strip():
                logger.warning(f"stderr:\n{stderr}")
            if exit_code != 0:
                logger.error(f"Docker command failed with exit code {exit_code}")

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
            command: The command to execute. Can contain {exe} and {local_path}
                placeholders.
            exe_name: Name of the executable to substitute for {exe}.
            kwargs: Additional keyword arguments for command formatting.
            local_path: Path used in command formatting.  In Docker mode the
                same path is accessible inside the container via the shared
                data volume, so no remapping is performed.
            cwd: Working directory for subprocess execution (ignored in Docker mode).
            logger: Dagster logger; when provided, output is forwarded to
                structured event logs.
            env: Environment variables for the subprocess or Docker container.

        Returns:
            CommandResult with returncode, stdout, and stderr.
        """
        final_command = self._build_command(command, exe_name, kwargs, local_path)

        if self.with_docker:
            return self._run_docker(final_command, env=env, logger=logger)
        elif logger is not None:
            return self._run_with_logging(final_command, cwd, env, logger)
        else:
            return self._run_direct(final_command, cwd, env)

    def version(self, exe: str, version_cmd: str = "--version") -> str:
        """Get version of an executable."""
        result = self.run(f"{{exe}} {version_cmd}", exe_name=exe)
        return format_version_stdout(result.stdout)


class ToolResource(ConfigurableResource):
    """Base resource for external tools that can run locally or in Docker.

    Subclasses must define ``exe_*`` fields and a ``_tool_defs`` ClassVar.
    Configure with either ``docker_image`` (Docker mode) or all ``exe_*``
    fields (local mode). Providing both or neither raises a ``ValueError``
    at instantiation time.

    ``_tool_defs`` maps logical tool names to ``(exe_field_name, docker_exe_name)``
    tuples, handling cases where the field name, local binary name, and Docker
    binary name all differ (e.g. LASTools 64-bit suffixes).
    """

    docker_image: str | None = Field(
        default=None,
        description=(
            "Docker image to run the tools in (Docker mode). "
            "Set this OR all exe_* fields below (local mode), not both."
        ),
    )

    # Subclasses MUST override: {tool_name: (exe_field_name, docker_exe_name)}
    _tool_defs: ClassVar[dict[str, tuple[str, str]]]

    @model_validator(mode="after")
    def _validate_execution_mode(self):
        has_docker = self.docker_image is not None
        exe_fields = [field_name for _, (field_name, _) in self._tool_defs.items()]
        missing = [f for f in exe_fields if getattr(self, f) is None]
        has_local = len(missing) == 0
        if has_docker and has_local:
            raise ValueError(
                f"{type(self).__name__}: set either 'docker_image' (Docker mode) "
                f"or exe_* fields (local mode), not both."
            )
        if not has_docker and not has_local:
            names = ", ".join(f"'{f}'" for f in exe_fields)
            raise ValueError(
                f"{type(self).__name__} requires either 'docker_image' (Docker mode) "
                f"or all of {names} (local mode)."
            )
        return self

    @property
    def with_docker(self) -> bool:
        return self.docker_image is not None

    @property
    def exes(self) -> Dict[str, str]:
        if self.with_docker:
            return {
                name: docker_exe for name, (_, docker_exe) in self._tool_defs.items()
            }
        return {
            name: getattr(self, field_name)
            for name, (field_name, _) in self._tool_defs.items()
        }

    @property
    def runner_config(self) -> tuple[Dict[str, str], str, bool]:
        return self.exes, self.docker_image or "", self.with_docker

    @property
    def runner(self) -> CommandRunner:
        exes, docker_image, with_docker = self.runner_config
        return CommandRunner(
            exes=exes, docker_image=docker_image, with_docker=with_docker
        )


class GDALResource(ToolResource):
    """GDAL resource providing ogr2ogr, ogrinfo, and sozip.

    Configure with local executable paths::

        gdal = GDALResource(
            exe_ogr2ogr="/usr/bin/ogr2ogr",
            exe_ogrinfo="/usr/bin/ogrinfo",
            exe_sozip="/usr/bin/sozip",
        )

    Or enable Docker mode to run GDAL in a standalone container::

        gdal = GDALResource(
            docker_image="ghcr.io/osgeo/gdal:ubuntu-small-3.8.5",
        )

    Acquire the runner with the ``runner`` property::

        gdal.runner
    """

    exe_ogr2ogr: str | None = Field(
        default=None,
        description="Full path to the ogr2ogr executable. Required for local mode.",
    )
    exe_ogrinfo: str | None = Field(
        default=None,
        description="Full path to the ogrinfo executable. Required for local mode.",
    )
    exe_sozip: str | None = Field(
        default=None,
        description="Full path to the sozip executable. Required for local mode.",
    )

    _tool_defs: ClassVar[dict[str, tuple[str, str]]] = {
        "ogr2ogr": ("exe_ogr2ogr", "ogr2ogr"),
        "ogrinfo": ("exe_ogrinfo", "ogrinfo"),
        "sozip": ("exe_sozip", "sozip"),
    }


class PDALResource(ToolResource):
    """PDAL resource providing the pdal executable.

    Configure with a local executable path::

        pdal = PDALResource(exe_pdal="/usr/bin/pdal")

    Or enable Docker mode::

        pdal = PDALResource(docker_image="pdal/pdal:2.8.4")

    Acquire the runner with the ``runner`` property::

        pdal.runner
    """

    exe_pdal: str | None = Field(
        default=None,
        description="Full path to the pdal executable. Required for local mode.",
    )

    _tool_defs: ClassVar[dict[str, tuple[str, str]]] = {
        "pdal": ("exe_pdal", "pdal"),
    }


class LASToolsResource(ToolResource):
    """LASTools resource providing lasindex, las2las, and lasinfo.

    Configure with local executable paths::

        lastools = LASToolsResource(
            exe_lasindex="/usr/bin/lasindex",
            exe_las2las="/usr/bin/las2las",
            exe_lasinfo="/usr/bin/lasinfo",
        )

    Or enable Docker mode::

        lastools = LASToolsResource(docker_image="3dgi/3dbag-pipeline-tools:latest")

    Acquire the runner with the ``runner`` property::

        lastools.runner
    """

    exe_lasindex: str | None = Field(
        default=None,
        description="Full path to the lasindex executable. Required for local mode.",
    )
    exe_las2las: str | None = Field(
        default=None,
        description="Full path to the las2las executable. Required for local mode.",
    )
    exe_lasinfo: str | None = Field(
        default=None,
        description="Full path to the lasinfo executable. Required for local mode.",
    )

    _tool_defs: ClassVar[dict[str, tuple[str, str]]] = {
        "lasindex": ("exe_lasindex", "lasindex64"),
        "las2las": ("exe_las2las", "las2las64"),
        "lasinfo": ("exe_lasinfo", "lasinfo64"),
    }


class TylerResource(ToolResource):
    """Tyler resource providing tyler, tyler-db, and tyler-multiformat.

    Configure with local executable paths::

        tyler = TylerResource(
            exe_tyler="/usr/bin/tyler",
            exe_tyler_db="/usr/bin/tyler-db",
            exe_tyler_multiformat="/usr/bin/tyler-multiformat",
        )

    Or enable Docker mode::

        tyler = TylerResource(docker_image="3dgi/tyler:0.3.14")

    Acquire the runner with the ``runner`` property::

        tyler.runner
    """

    exe_tyler: str | None = Field(
        default=None,
        description="Full path to the tyler executable. Required for local mode.",
    )
    exe_tyler_db: str | None = Field(
        default=None,
        description="Full path to the tyler-db executable. Required for local mode.",
    )
    exe_tyler_multiformat: str | None = Field(
        default=None,
        description="Full path to the tyler-multiformat executable. Required for local mode.",
    )

    _tool_defs: ClassVar[dict[str, tuple[str, str]]] = {
        "tyler": ("exe_tyler", "tyler"),
        "tyler-db": ("exe_tyler_db", "tyler-db"),
        "tyler-multiformat": ("exe_tyler_multiformat", "tyler-multiformat"),
    }


class ValidationResource(ToolResource):
    """Validation resource providing val3dity, cjval, and cjio.

    Configure with local executable paths::

        validation = ValidationResource(
            exe_val3dity="/usr/bin/val3dity",
            exe_cjval="/usr/bin/cjval",
            exe_cjio="/usr/bin/cjio",
        )

    Or enable Docker mode::

        validation = ValidationResource(docker_image="tudelft3d/cjval:0.8.2")

    Acquire the runner with the ``runner`` property::

        validation.runner
    """

    exe_val3dity: str | None = Field(
        default=None,
        description="Full path to the val3dity executable. Required for local mode.",
    )
    exe_cjval: str | None = Field(
        default=None,
        description="Full path to the cjval executable. Required for local mode.",
    )
    exe_cjio: str | None = Field(
        default=None,
        description="Full path to the cjio executable. Required for local mode.",
    )

    _tool_defs: ClassVar[dict[str, tuple[str, str]]] = {
        "val3dity": ("exe_val3dity", "val3dity"),
        "cjval": ("exe_cjval", "cjval"),
        "cjio": ("exe_cjio", "cjio"),
    }


class RooferResource(ToolResource):
    """Roofer resource providing the crop and roofer executables.

    Configure with local executable paths::

        roofer = RooferResource(
            exe_crop="/usr/bin/crop",
            exe_roofer="/usr/bin/roofer",
        )

    Or enable Docker mode::

        roofer = RooferResource(docker_image="3dgi/roofer:develop")

    Acquire the runner with the ``runner`` property::

        roofer.runner
    """

    exe_crop: str | None = Field(
        default=None,
        description="Full path to the crop executable. Required for local mode.",
    )
    exe_roofer: str | None = Field(
        default=None,
        description="Full path to the roofer executable. Required for local mode.",
    )

    _tool_defs: ClassVar[dict[str, tuple[str, str]]] = {
        "crop": ("exe_crop", "crop"),
        "roofer": ("exe_roofer", "roofer"),
    }


class GeoflowResource(ToolResource):
    """Geoflow resource providing the geof executable and reconstruction flowchart.

    Configure with local executable path and flowchart::

        geoflow = GeoflowResource(
            exe_geoflow="/usr/bin/geof",
            flowchart="/path/to/flowchart.json",
        )

    Or enable Docker mode (flowchart is still required)::

        geoflow = GeoflowResource(
            docker_image="3dgi/geoflow-bundle-builder:2025.09.01",
            flowchart="/path/to/flowchart.json",
        )

    Acquire the runner with the ``runner`` property::

        geoflow.runner
    """

    exe_geoflow: str | None = Field(
        default=None,
        description="Full path to the geof executable. Required for local mode.",
    )
    flowchart: str | None = Field(
        default=None,
        description=(
            "Full path to the reconstruction flowchart JSON file. "
            "Required in both Docker and local mode."
        ),
    )

    _tool_defs: ClassVar[dict[str, tuple[str, str]]] = {
        "geof": ("exe_geoflow", "geof"),
    }

    @model_validator(mode="after")
    def _validate_flowchart(self):
        if self.flowchart is None:
            raise ValueError(
                "GeoflowResource requires 'flowchart' in both Docker and local mode."
            )
        return self
