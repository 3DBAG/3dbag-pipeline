import os
from pathlib import Path
from typing import Tuple
from copy import deepcopy
import signal
from subprocess import PIPE, STDOUT, Popen

import docker
from dagster import (get_dagster_logger, resource, Field, Noneable, Failure)
from dagster_shell import execute_shell_command
from docker.errors import ImageNotFound

DOCKER_PDAL_IMAGE = "pdal/pdal:sha-cfa827b6"  # PDAL 2.4.3
DOCKER_GDAL_IMAGE = "osgeo/gdal:alpine-small-3.5.2"


def execute_shell_command_silent(shell_command: str,
                                 cwd=None,
                                 env=None
                                 ):
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


class AppImage:
    """An application, either as paths of executables, or as a docker image."""

    def __init__(self, exes: dict, docker_cfg=None, with_docker: bool = False,
                 kwargs: dict = None):
        self.logger = get_dagster_logger()
        self.exes = exes
        self.with_docker = with_docker
        self.kwargs = kwargs
        if self.with_docker:
            self.docker_client = docker.from_env()
            try:
                self.docker_image = self.docker_client.images.get(docker_cfg["image"])
            except ImageNotFound:
                self.docker_image = self.docker_client.images.pull(docker_cfg["image"])
            self.container_mount_point = Path(docker_cfg["mount_point"])
        else:
            self.docker_client = None
            self.docker_image = None
            self.container_mount_point = None

    def execute(self, exe_name: str, command: str, kwargs: dict = None,
                local_path: Path = None, silent=False, cwd: str = None) -> Tuple[int, str]:
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
                raise ValueError("Cannot include 'exe' in the kwargs. Pass the exe in "
                                 "the 'exe_name' parameter.")
            if "local_path" in kwargs:
                raise ValueError("Cannot include 'local_path' in the kwargs. Pass the "
                                 "path in the 'local path' parameter.")
        kwargs_with_exe = deepcopy(kwargs) if kwargs else dict()
        kwargs_with_exe["exe"] = self.exes[exe_name]
        if self.with_docker:
            if local_path:
                if local_path.is_dir():
                    container_path = self.container_mount_point
                else:
                    container_path = self.container_mount_point / local_path.name
                kwargs_with_exe["local_path"] = container_path
                volumes = [f"{local_path}:{container_path}", ]
            else:
                volumes = None
            output = self._docker_run(command.format(**kwargs_with_exe),
                                      volumes=volumes)
            return_code = 1 if "error" in output.lower() else 0
        else:
            kwargs_with_exe["local_path"] = local_path
            if silent:
                output, return_code = execute_shell_command_silent(
                    shell_command=command.format(**kwargs_with_exe),
                    cwd=cwd
                )
            else:
                output, return_code = execute_shell_command(
                    shell_command=command.format(**kwargs_with_exe),
                    log=self.logger,
                    output_logging="STREAM",
                    cwd=cwd
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
                self.docker_image, command=command, volumes=volumes,
                network_mode="host", remove=True, detach=False, stdout=True,
                stderr=True
            )
            return stdout.decode("utf-8")
        else:
            raise RuntimeError("executable resource was not initialized with a "
                               "docker image")


@resource(
    description="GDAL executables, either local or in a docker image. Defaults to ogr* "
                "that is in the path.",
    config_schema={
        "exes": {
            "ogr2ogr": Field(
                Noneable(str), default_value=None,
                description="Path to the ogr2ogr executable"
            ),
            "ogrinfo": Field(
                Noneable(str), default_value=None,
                description="Path to the ogrinfo executable"
            ),
            "sozip": Field(
                Noneable(str), default_value=None,
                description="Path to the sozip executable"
            )
        },
        "docker": {
            "image": Field(
                str, default_value=DOCKER_GDAL_IMAGE,
                description="Docker image reference"
            ),
            "mount_point": Field(
                str, default_value="/tmp",
                description=("The mount point in the container where the data "
                             "directory from the host is bind mounted.")
            )
        }
    },
)
def gdal(context):
    """GDAL executables in a docker image or locally. Defaults to using a docker
    image."""
    gdal_exes = {k: v for k, v in context.resource_config.get("exes").items()}
    if all(v is None for v in gdal_exes.values()):
        with_docker = True
        gdal_exes["ogrinfo"] = "ogrinfo"
        gdal_exes["ogr2ogr"] = "ogr2ogr"
        gdal_exes["sozip"] = "sozip"
    else:
        with_docker = False
    return AppImage(exes=gdal_exes,
                    docker_cfg=context.resource_config.get("docker"),
                    with_docker=with_docker)


@resource(
    config_schema={
        "exes": {
            "partialzip": Field(
                Noneable(str), default_value="partialzip",
                description="Path to the partialzip executable"
            ),
        },
        "docker": {
            "image": Field(
                str,
                description="Docker image reference"
            ),
            "mount_point": Field(
                str, default_value="/tmp",
                description=("The mount point in the container where the data "
                             "directory from the host is bind mounted.")
            )
        }
    }
)
def partialzip(context):
    """partialzip (https://crates.io/crates/partialzip) executables, either local or
    in a docker image. Defaults to 'partialzip' that is in the path."""
    partialzip_exe = {k: v for k, v in context.resource_config.get("exes").items()}
    if partialzip_exe["partialzip"]:
        with_docker = False
    else:
        with_docker = True
        partialzip_exe["partialzip"] = "partialzip"
    return AppImage(exes=partialzip_exe,
                    docker_cfg=context.resource_config.get("docker"),
                    with_docker=with_docker)


@resource(
    config_schema={
        "exes": {
            "pdal": Field(
                Noneable(str), default_value=None,
                description="Path to the pdal executable"
            ),
        },
        "docker": {
            "image": Field(
                str, default_value=DOCKER_PDAL_IMAGE,
                description="Docker image reference"
            ),
            "mount_point": Field(
                str, default_value="/tmp",
                description=("The mount point in the container where the data "
                             "directory from the host is bind mounted.")
            )
        }
    }
)
def pdal(context):
    """PDAL executables in a docker image or a local executable. Defaults to using a
    docker image."""
    pdal_exe = {k: v for k, v in context.resource_config.get("exes").items()}
    if pdal_exe["pdal"]:
        with_docker = False
    else:
        with_docker = True
        pdal_exe["pdal"] = "pdal"
    return AppImage(exes=pdal_exe,
                    docker_cfg=context.resource_config.get("docker"),
                    with_docker=with_docker)


@resource(
    description="LASTools executables on the local system.",
    config_schema={
        "exes": {
            "lasindex": Field(
                Noneable(str), default_value=None,
                description="Path to the lasindex executable"
            ),
            "las2las": Field(
                Noneable(str), default_value=None,
                description="Path to the las2las executable"
            )
        },
    },
)
def lastools(context):
    """LASTools executables on the local system."""
    lastools_exes = {k: v for k, v in context.resource_config.get("exes").items()}
    return AppImage(exes=lastools_exes,
                    with_docker=False)


@resource(
    description="Tyler executables on the local system.",
    config_schema={
        "exes": {
            "tyler-db": Field(
                Noneable(str), default_value=None,
                description="Path to the tyler-db executable"
            ),
            "tyler": Field(
                Noneable(str), default_value=None,
                description="Path to the tyler executable"
            ),
        },
    },
)
def tyler(context):
    """Tyler executables on the local system."""
    tyler_exes = {k: v for k, v in context.resource_config.get("exes").items()}
    app_image = AppImage(exes=tyler_exes, with_docker=False)
    return app_image


@resource(
    description="Roofer executables on the local system.",
    config_schema={
        "exes": {
            "crop": Field(
                Noneable(str), default_value=None,
                description="Path to the roofer crop executable"
            ),
        },
    },
)
def roofer(context):
    """Roofer executables on the local system."""
    roofer_exes = {k: v for k, v in context.resource_config.get("exes").items()}
    return AppImage(exes=roofer_exes,
                    with_docker=False)


@resource(
    description="Geoflow executable on the local system.",
    config_schema={
        "exes": {
            "geof": Field(
                Noneable(str), default_value=None,
                description="Path to the geof executable"
            ),
        },
        "flowcharts": {
            "reconstruct": Field(
                Noneable(str), default_value=None,
                description="Path to the reconstruct flowchart"
            )
        }
    },
)
def geoflow(context):
    """Geoflow executable on the local system."""
    geoflow_exes = {k: v for k, v in context.resource_config.get("exes").items()}
    return AppImage(exes=geoflow_exes, with_docker=False,
                    kwargs={"flowcharts": context.resource_config.get("flowcharts")})
