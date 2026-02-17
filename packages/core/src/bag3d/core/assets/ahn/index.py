from dagster import asset, get_dagster_logger, Config, AssetExecutionContext
from pydantic import Field

from bag3d.common.resources.executables import LASToolsResource
from bag3d.core.assets.ahn.core import partition_definition_ahn
from bag3d.core.assets.ahn.download import LAZDownload

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


def run_lasindex(
    context: AssetExecutionContext,
    config: LasIndexConfig,
    lastools: LASToolsResource,
    lazdownload: LAZDownload,
):
    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-tile_size",
        str(config.tile_size),
    ]
    if not config.force:
        cmd_list.append("-dont_reindex")
    lastools.runner.run(
        " ".join(cmd_list),
        exe_name="lasindex",
        local_path=lazdownload.path,
        logger=logger,
    )


@asset(
    partitions_def=partition_definition_ahn,
    pool="ahn",
)
def lasindex_ahn3(
    context: AssetExecutionContext,
    config: LasIndexConfig,
    lastools: LASToolsResource,
    laz_files_ahn3: LAZDownload,
):
    """Append a spatial index to the AHN3 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    run_lasindex(context, config, lastools, laz_files_ahn3)


@asset(
    partitions_def=partition_definition_ahn,
    pool="ahn",
)
def lasindex_ahn4(
    context: AssetExecutionContext,
    config: LasIndexConfig,
    lastools: LASToolsResource,
    laz_files_ahn4: LAZDownload,
):
    """Append a spatial index to the AHN4 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    run_lasindex(context, config, lastools, laz_files_ahn4)


@asset(
    partitions_def=partition_definition_ahn,
    pool="ahn",
)
def lasindex_ahn5(
    context: AssetExecutionContext,
    config: LasIndexConfig,
    lastools: LASToolsResource,
    laz_files_ahn5: LAZDownload,
):
    """Append a spatial index to the AHN5 LAZ file, using LASTools's `lasindex`.

    See https://lastools.osgeo.org/download/lasindex_README.txt.
    """
    run_lasindex(context, config, lastools, laz_files_ahn5)
