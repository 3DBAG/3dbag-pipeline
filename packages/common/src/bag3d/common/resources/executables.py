import os
from pathlib import Path
from typing import Tuple
from copy import deepcopy
import signal
from subprocess import PIPE, STDOUT, Popen
from typing import Dict, Optional

from dagster import (
    get_dagster_logger,
    resource,
    Field,
    Noneable,
    Failure,
    ConfigurableResource,
)
from dagster_shell import execute_shell_command
import docker
from docker.errors import ImageNotFound

DOCKER_PDAL_IMAGE = "pdal/pdal:sha-cfa827b6"  # PDAL 2.4.3
DOCKER_GDAL_IMAGE = "ghcr.io/osgeo/gdal:ubuntu-small-latest"


def execute_shell_command_silent(shell_command: str, cwd=None, env=None):
    """Execute a shell command without sending the output to the logger, and without
    writing the command to a script file first.

    NOTE: This function is based on the execute_script_file dagster_shell function,
    which can be found here: https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-shell/dagster_shell/utils.py

    Args:
        shell_command (str): The shell script to execute.
        cwd (str, optional): Working directory for the shell command to use.
        env (Dict[str, str], optional): Environment dictionary to pass to ``subprocess.Popen``.
            Unused by default.

    Returns:
        Tuple[str, int]: A tuple where the first element is the combined
        stdout/stderr output of running the shell command and the second element is
        the return code.
    """

    def pre_exec():
        # Restore default signal disposition and invoke setsid
        for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), signal.SIG_DFL)
        os.setsid()

    sub_process = None
    try:
        stdout_pipe = PIPE
        stderr_pipe = STDOUT

        sub_process = Popen(
            shell_command,
            shell=True,
            stdout=stdout_pipe,
            stderr=stderr_pipe,
            cwd=cwd,
            env=env,
            preexec_fn=pre_exec,
            encoding="UTF-8",
        )

        # Stream back logs as they are emitted
        lines = []
        for line in sub_process.stdout:
            lines.append(line)
        output = "".join(lines)

        sub_process.wait()

        return output, sub_process.returncode
    finally:
        # Always terminate subprocess, including in cases where the run is terminated
        if sub_process:
            sub_process.terminate()


def format_version_stdout(version: str) -> str:
    return version.replace("\n", ",")


class DockerConfig(ConfigurableResource):
    image: str
    mount_point: str


class AppImage:
    """An application, either as paths of executables, or as a docker image."""

    def __init__(
        self,
        exes: dict,
        docker_cfg: DockerConfig = None,
        with_docker: bool = False,
        kwargs: dict = None,
    ):
        self.logger = get_dagster_logger()
        self.exes = exes
        self.with_docker = with_docker
        self.kwargs = kwargs
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

    def execute(
        self,
        exe_name: str,
        command: str,
        kwargs: dict = None,
        local_path: Path = None,
        silent=False,
        cwd: str = None,
    ) -> Tuple[int, str]:
        """Execute a command in a docker container if an image is available, otherwise
        execute with the local executable.

        Since ``execute`` selects the correct executable, based on the availability of
        a docker image, you always need to pass an ``exe`` placeholder in the
        ``command`` string. The ``exe`` will be substituted internally by ``execute``.
        For instance ``"{exe} --version"``, to get the version of an executable.

        If the command needs access to a path, the second placeholder that must be in
        the ``command`` string is ``local_path``.
        The ``local_path`` in the command string will be substituted with the
        ``local_path`` parameter value, if the executable is local.
        If the command runs in docker, the ``local_path`` in the command string takes
        the value of the ``container_path``.

        The ``container_path`` is computed from the ``local_path`` parameter and the
        ``mount_point`` configuration value, such that
        ``container_path = mount_point / local_path.name``.

        Args:
            exe_name: Name of the executable to run (as defined in the resource config
                { "exes": { <name>: ...} }). This is because an AppImage can use
                multiple executables, and you need to select the one that needs to be
                run.
            command: The command to be executed. It is formatted with the ``kwargs``
                and the executable. Therefore, it must contain a placeholder for
                ``exe``, which is substitued from ``self.exe``.
            kwargs: Keyword arguments to pass as command parameters.
            local_path: If ``local_path`` is a directory, then it is mounted on the
                ``mount_point`` directly. If ``local_path`` is a file, then it is
                mounted on the ``mount_point`` as
                ``local_path : mount_point/local_path.name``.
            silent: If False, send execution messages to the logger, else do not log.

        Returns:
             The STDOUT and return code from the command execution.

        Examples:

            .. code-block:: python

                # Pass the name of the exe first, then the command, including the
                # 'exe' placeholder.
                self.execute("ogrinfo", "{exe} --version")

                self.execute("ogrinfo", "{exe} -so -al {local_path}",
                             local_path=Path("/tmp/myfile.gml"))
        """
        if kwargs:
            if "exe" in kwargs:
                raise ValueError(
                    "Cannot include 'exe' in the kwargs. Pass the exe in "
                    "the 'exe_name' parameter."
                )
            if "local_path" in kwargs:
                raise ValueError(
                    "Cannot include 'local_path' in the kwargs. Pass the "
                    "path in the 'local path' parameter."
                )
        kwargs_with_exe = deepcopy(kwargs) if kwargs else dict()
        kwargs_with_exe["exe"] = self.exes[exe_name]
        if self.with_docker:
            if local_path:
                if local_path.is_dir():
                    container_path = self.container_mount_point
                else:
                    container_path = self.container_mount_point / local_path.name
                kwargs_with_exe["local_path"] = container_path
                volumes = [
                    f"{local_path}:{container_path}",
                ]
            else:
                volumes = None
            output = self._docker_run(
                command.format(**kwargs_with_exe), volumes=volumes
            )
            return_code = 1 if "error" in output.lower() else 0
        else:
            kwargs_with_exe["local_path"] = local_path
            if silent:
                output, return_code = execute_shell_command_silent(
                    shell_command=command.format(**kwargs_with_exe), cwd=cwd
                )
            else:
                output, return_code = execute_shell_command(
                    shell_command=command.format(**kwargs_with_exe),
                    log=self.logger,
                    output_logging="STREAM",
                    cwd=cwd,
                )
        if return_code != 0:
            raise Failure(f"{kwargs_with_exe['exe']} failed with output:\n{output}")
        elif "error" in output.lower():
            self.logger.error(f"Error in subprocess output: {output}")
            return return_code, output
        else:
            return return_code, output

    def _docker_run(self, command, volumes=None, silent=False) -> str:
        """Executes a `command` with 'docker run'.

        The `host_path` is mounted at `mount_point` that is provided in the resource
            configuration.
        The `--network` is set to `host`.
        Removes the container when finished.

        Returns the STDOUT from the container.
        """
        if self.with_docker:
            if not silent:
                logger = get_dagster_logger()
                logger.info(f"Executing `{command}` in {self.docker_image.tags}")
            stdout = self.docker_client.containers.run(
                self.docker_image,
                command=command,
                volumes=volumes,
                network_mode="host",
                remove=True,
                detach=False,
                stdout=True,
                stderr=True,
            )
            return stdout.decode("utf-8")
        else:
            raise RuntimeError(
                "executable resource was not initialized with a " "docker image"
            )

    def version(self, exe: str):
        version, returncode = execute_shell_command_silent(f"{exe} --version")
        return format_version_stdout(version)


class GDALResources(ConfigurableResource):
    """
    A GDAL Resource can be configured by either the local EXE paths
    for `ogr2ogr`, `ogrinfo` and `sozip`, or by providing the DockerConfig
    for the GDAL image.

    For the local exes you can use:

        gdal_resource = GDALResources(exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
                                     exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
                                     exe_sozip=os.getenv("EXE_PATH_SOZIP"))

    For the docker image you can use:

        gdal_local = GDALResources(docker_cfg=DockerConfig(
                                image=DOCKER_GDAL_IMAGE,
                                mount_point="/tmp"))

    If instantiated with GDALResources() then the Docker image is used by
    default. After the resource has been instantiated, gdal (AppImage) can
    be acquired with the `app` property:

        gdal_resource.app
    """

    exe_ogrinfo: str
    exe_ogr2ogr: str
    exe_sozip: str
    docker_cfg: DockerConfig

    def __init__(
        self,
        exe_ogrinfo: Optional[str] = None,
        exe_ogr2ogr: Optional[str] = None,
        exe_sozip: Optional[str] = None,
        docker_cfg: Optional[DockerConfig] = None,
    ):
        super().__init__(
            exe_ogrinfo=exe_ogrinfo or "ogrinfo",
            exe_ogr2ogr=exe_ogr2ogr or "ogr2ogr",
            exe_sozip=exe_sozip or "sozip",
            docker_cfg=docker_cfg
            or DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp"),
        )

    @property
    def exes(self) -> Dict[str, str]:
        return {
            "ogrinfo": self.exe_ogrinfo,
            "ogr2ogr": self.exe_ogr2ogr,
            "sozip": self.exe_sozip,
        }

    @property
    def with_docker(self) -> bool:
        if (
            self.exe_ogrinfo == "ogrinfo"
            and self.exe_ogr2ogr == "ogr2ogr"
            and self.exe_sozip == "sozip"
        ):
            return True
        else:
            return False

    @property
    def app(self) -> AppImage:
        return AppImage(
            exes=self.exes, docker_cfg=self.docker_cfg, with_docker=self.with_docker
        )


class PDALResources(ConfigurableResource):
    """
    A PDAL Resource can be configured by either the local EXE path
    for `pdal` or by providing the DockerConfig for the PDAL image.

    For the local exe you can use:

        pdal_resource = PDALResources(exe_pdal=os.getenv("EXE_PATH_PDAL"))

    For the docker image you can use:

        pdal_resource = PDALResources(docker_cfg=DockerConfig(
                                        image=DOCKER_PDAL_IMAGE,
                                        mount_point="/tmp"))

    If instantiated with PDALResources() then the Docker image is used by
    default. After the resource has been instantiated, pdal (AppImage) can
    be acquired with the `app` property:

        pdal_resource.app
    """

    exe_pdal: str
    docker_cfg: DockerConfig

    def __init__(
        self,
        exe_pdal: Optional[str] = None,
        docker_cfg: Optional[DockerConfig] = None,
    ):
        super().__init__(
            exe_pdal=exe_pdal or "pdal",
            docker_cfg=docker_cfg
            or DockerConfig(image=DOCKER_PDAL_IMAGE, mount_point="/tmp"),
        )

    @property
    def exes(self) -> Dict[str, str]:
        return {
            "pdal": self.exe_pdal,
        }

    @property
    def with_docker(self) -> bool:
        if self.exe_pdal == "pdal":
            return True
        else:
            return False

    @property
    def app(self) -> AppImage:
        return AppImage(
            exes=self.exes, docker_cfg=self.docker_cfg, with_docker=self.with_docker
        )


class LASToolsResource(ConfigurableResource):
    """
    A LASTools Resource can be configured by providing the paths to
    LASTools executables "lasindex" and "las2las" on the local system.

    Example:

        lastools_resource = LASToolsResource(exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
                                             exe_las2las=s.getenv("EXE_PATH_LAS2LAS"))

    After the resource has been instantiated, lastools (AppImage) can
    be acquired with the `app` property:

        lastools_resource.app
    """

    exe_lasindex: str
    exe_las2las: str

    @property
    def exes(self) -> Dict[str, str]:
        return {"lasindex": self.exe_lasindex, "las2las": self.exe_las2las}

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def app(self) -> AppImage:
        return AppImage(exes=self.exes, with_docker=self.with_docker)


class TylerResource(ConfigurableResource):
    """
    A Tyler Resource can be configured by providing the paths to
    Tyler executables "tyler" and "tyler-db" on the local system.

    Example:

        tyler_resource = TylerResource(exe_tyler=os.getenv("EXE_PATH_TYLER"),
                                       exe_tyler_db=s.getenv("EXE_PATH_TYLER_DB"))

    After the resource has been instantiated, tyler (AppImage) can
    be acquired with the `app` property:

        tyler = tyler_resource.app
    """

    exe_tyler: str
    exe_tyler_db: str

    def exes(self) -> Dict[str, str]:
        return {"tyler-db": self.exe_tyler, "tyler": self.exe_tyler_db}

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def app(self) -> AppImage:
        return AppImage(exes=self.exes, with_docker=self.with_docker)


class RooferResource(ConfigurableResource):
    """
    A RooferResource can be configured by providing the paths to
    Roofer `crop` executable on the local system.

    Example:

        roofer_resource = RooferResource(exe_roofer_crop=os.getenv("EXE_PATH_ROOFER_CROP"))

    After the resource has been instantiated, roofer (AppImage) can
    be acquired with the `app` property:

        roofer = roofer_resource.app
    """

    exe_roofer_crop: str

    def exes(self) -> Dict[str, str]:
        return {"crop": self.exe_roofer_crop}

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def app(self) -> AppImage:
        return AppImage(exes=self.exes, with_docker=self.with_docker)


class GeoflowResource(ConfigurableResource):
    """
    A GeoflowResource can be configured by providing the paths to
    Geoflow `exe_geoflow` executable on the local system
    and the path to the reconstruction flowchart.

    Example:

        geoflow_resource = GeoflowResource(exe_geoflow = os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
                                           flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"))

    After the resource has been instantiated, geoflow (AppImage) can
    be acquired with the `app` property:

        geoflow = geoflow_resource.app
    """

    exe_geoflow: str
    flowchart: str

    def exes(self) -> Dict[str, str]:
        return {"geof": self.exe_geoflow}

    @property
    def with_docker(self) -> bool:
        return False

    @property
    def app(self) -> AppImage:
        return AppImage(
            exes=self.exes,
            with_docker=self.with_docker,
            kwargs={"flowcharts": {"reconstruct": self.flowchart}},
        )
