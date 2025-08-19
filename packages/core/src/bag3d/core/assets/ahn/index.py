from dagster import asset, get_dagster_logger, Config
from pydantic import Field

from bag3d.core.assets.ahn.core import partition_definition_ahn


logger = get_dagster_logger("ahn.index")


class LasIndexConfig(Config):
    tile_size: int = Field(
        default=10,
        description="Set smallest spatial area indexed to tile_size by tile_size units.",
    )
    force: bool = Field(
        default=False,
        description="Force re-index the file, even if it is already indexed.",
    )
    verbose: bool = Field(
        default=False,
        description="Output stdout/stderr from lasindex",
    )


@asset(
    required_resource_keys={"lastools"},
    partitions_def=partition_definition_ahn,
)
def lasindex_ahn3(context, config: LasIndexConfig, laz_files_ahn3):
    """Append a spatial index to the AHN3 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    silent = not config.verbose
    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-tile_size",
        str(config.tile_size),
    ]
    if not config.force:
        cmd_list.append("-dont_reindex")
    context.resources.lastools.app.execute(
        "lasindex", " ".join(cmd_list), local_path=laz_files_ahn3.path, silent=silent
    )


@asset(
    required_resource_keys={"lastools"},
    partitions_def=partition_definition_ahn,
)
def lasindex_ahn4(context, config: LasIndexConfig, laz_files_ahn4):
    """Append a spatial index to the AHN4 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    silent = not config.verbose
    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-tile_size",
        str(config.tile_size),
    ]
    if not config.force:
        cmd_list.append("-dont_reindex")
    context.resources.lastools.app.execute(
        "lasindex", " ".join(cmd_list), local_path=laz_files_ahn4.path, silent=silent
    )


@asset(
    required_resource_keys={"lastools"},
    partitions_def=partition_definition_ahn,
)
def lasindex_ahn5(context, config: LasIndexConfig, laz_files_ahn5):
    """Append a spatial index to the AHN5 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    silent = not config.verbose
    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-tile_size",
        str(config.tile_size),
    ]
    if not config.force:
        cmd_list.append("-dont_reindex")
    context.resources.lastools.app.execute(
        "lasindex", " ".join(cmd_list), local_path=laz_files_ahn5.path, silent=silent
    )
