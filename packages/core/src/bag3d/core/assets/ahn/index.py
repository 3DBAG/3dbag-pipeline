from dagster import asset, Field, get_dagster_logger

from bag3d.core.assets.ahn.core import partition_definition_ahn


logger = get_dagster_logger("ahn.index")


@asset(
    config_schema={
        "tile_size": Field(
            int,
            default_value=250,
            description="Set smallest spatial area indexed to tile_size by tile_size units.",
        ),
        "force": Field(
            bool,
            default_value=False,
            description="Force the re-index the file, even if it is already indexed.",
        ),
    },
    required_resource_keys={"lastools"},
    partitions_def=partition_definition_ahn,
)
def lasindex_ahn3(context, laz_files_ahn3):
    """Append a spatial index to the AHN3 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-append",
        "-tile_size",
        str(context.op_execution_context.op_config["tile_size"]),
    ]
    if context.op_execution_context.op_config["force"] is False:
        cmd_list.append("-dont_reindex")
    context.resources.lastools.app.execute(
        "lasindex", " ".join(cmd_list), local_path=laz_files_ahn3.path
    )


@asset(
    config_schema={
        "tile_size": Field(
            int,
            default_value=250,
            description="Set smallest spatial area indexed to tile_size by tile_size units.",
        ),
        "force": Field(
            bool,
            default_value=False,
            description="Force the re-index the file, even if it is already indexed.",
        ),
    },
    required_resource_keys={"lastools"},
    partitions_def=partition_definition_ahn,
)
def lasindex_ahn4(context, laz_files_ahn4):
    """Append a spatial index to the AHN4 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-append",
        "-tile_size",
        str(context.op_execution_context.op_config["tile_size"]),
    ]
    if context.op_execution_context.op_config["force"] is False:
        cmd_list.append("-dont_reindex")
    context.resources.lastools.app.execute(
        "lasindex", " ".join(cmd_list), local_path=laz_files_ahn4.path
    )


@asset(
    config_schema={
        "tile_size": Field(
            int,
            default_value=250,
            description="Set smallest spatial area indexed to tile_size by tile_size units.",
        ),
        "force": Field(
            bool,
            default_value=False,
            description="Force the re-index the file, even if it is already indexed.",
        ),
    },
    required_resource_keys={"lastools"},
    partitions_def=partition_definition_ahn,
)
def lasindex_ahn5(context, laz_files_ahn5):
    """Append a spatial index to the AHN5 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-append",
        "-tile_size",
        str(context.op_execution_context.op_config["tile_size"]),
    ]
    if context.op_execution_context.op_config["force"] is False:
        cmd_list.append("-dont_reindex")
    context.resources.lastools.app.execute(
        "lasindex", " ".join(cmd_list), local_path=laz_files_ahn5.path
    )
