"""A temporary workaround so that we can get the exe versions as asset code versions.
Need to migrate the whole resources and repository definitions to the new
ConfigurableResource and Definitions API,
https://docs.dagster.io/concepts/resources#resources.

!!! IMPORTANT !!!
There is an ongoing discussion on automatic code versioning.
Read the issue for the disadvantages of the approach.
https://github.com/dagster-io/dagster/issues/15242
"""
from bag3d.common.resources.executables import execute_shell_command_silent

EXE_PATH_TYLER = "/opt/bin/tyler"
EXE_PATH_TYLER_DB = "/opt/bin/tyler-db"
EXE_PATH_ROOFER_CROP = "/opt/bin/crop"
EXE_PATH_GEOF = "/opt/bin/geof"
FLOWCHART_PATH_RECONSTRUCT = "/opt/geoflow-flowcharts/gfc-brecon/stream/reconstruct_bag.json"
EXE_PATH_GDAL = "/opt/bin/ogr2ogr"
EXE_PATH_PDAL = "/opt/bin/pdal"
EXE_PATH_LASTOOLS = "/opt/bin/las2las64"


def format_version_stdout(version: str) -> str:
    return version.replace("\n", ",")


def tyler_version():
    version, returncode = execute_shell_command_silent(f"{EXE_PATH_TYLER} --version")
    return format_version_stdout(version)


def tyler_db_version():
    version, returncode = execute_shell_command_silent(f"{EXE_PATH_TYLER_DB} --version")
    return format_version_stdout(version)


def roofer_version():
    version, returncode = execute_shell_command_silent(
        f"{EXE_PATH_ROOFER_CROP} --version")
    return format_version_stdout(version)


def geoflow_version():
    version_geof, returncode = execute_shell_command_silent(
        f"{EXE_PATH_GEOF} --version --verbose")
    version_plugins, returncode = execute_shell_command_silent(
        f"{EXE_PATH_GEOF} --list-plugins --verbose")
    gv = version_geof.strip().replace("\n", ", ")
    plugin_versions = version_plugins.find(" >")
    pv = version_plugins[plugin_versions:].strip().replace("\n   ", ", ").replace("\n",
                                                                                  ",")
    version = f"{gv}. Plugins: {pv}"
    return format_version_stdout(version)


def gdal_version():
    version, returncode = execute_shell_command_silent(
        f"{EXE_PATH_GDAL} --version")
    return format_version_stdout(version)


def pdal_version():
    version, returncode = execute_shell_command_silent(
        f"{EXE_PATH_PDAL} --version")
    return version.replace("-", "").replace("\n", "")


def lastools_version():
    version, returncode = execute_shell_command_silent(
        f"{EXE_PATH_LASTOOLS} -version")
    return format_version_stdout(version)
