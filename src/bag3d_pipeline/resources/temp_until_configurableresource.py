"""A temporary workaround so that we can get the exe versions as asset code versions.
Need to migrate the whole resources and repository definitions to the new
ConfigurableResource and Definitions API,
https://docs.dagster.io/concepts/resources#resources.
"""
from bag3d_pipeline.resources.executables import execute_shell_command_silent

EXE_PATH_TYLER = "/opt/bin/tyler"
EXE_PATH_TYLER_DB = "/opt/bin/tyler-db"
EXE_PATH_ROOFER_CROP = "/opt/bin/crop"
EXE_PATH_GEOF = "/opt/bin/geof"
FLOWCHART_PATH_RECONSTRUCT = "/opt/geoflow-flowcharts/gfc-brecon/stream/reconstruct_bag.json"


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
    version, returncode = execute_shell_command_silent(
        f"{EXE_PATH_GEOF} --list-plugins --verbose")
    return format_version_stdout(version)
