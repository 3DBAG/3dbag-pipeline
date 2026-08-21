"""IFC tile export."""

import multiprocessing as mp
from os import getenv
from pathlib import Path

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from dagster import AssetKey, Config, asset, get_dagster_logger
from pydantic import Field

from bag3d.export.ifc.convert import convert_cityjson_to_ifc

logger = get_dagster_logger("export.ifc")


class IFCConfig(Config):
    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_IFC", "1")),
        description="Number of parallel workers for IFC conversion",
    )
    ignore_duplicate_keys: bool = Field(
        default=False,
        description="Ignore duplicate JSON keys in the CityJSON tiles",
    )


def _convert_tile(args: tuple[Path, bool]) -> list[Path]:
    cityjson_path, ignore_duplicate_keys = args
    return convert_cityjson_to_ifc(
        cityjson_path, ignore_duplicate_keys=ignore_duplicate_keys
    )


@asset(
    deps={AssetKey(("export", "reconstruction_output_cityjson"))},
    pool="ifc",
)
def reconstruction_output_ifc(
    config: IFCConfig,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
) -> None:
    """Tiles for distribution in IFC format.

    Each CityJSON tile is converted to one IFC file per LoD (0, 1.2, 1.3, 2.2).
    The resulting ``.ifc`` files are zipped into a single ``.ifc.zip`` per tile
    by ``compressed_tiles``. Runs after the CityJSON tiles are generated and
    before they are gzipped by ``compressed_tiles``.
    """
    export_dir = file_store.stage_subdir("export", version.version)
    cityjson_files = sorted(export_dir.glob("t/**/*.city.json"))
    if not cityjson_files:
        raise FileNotFoundError(f"No CityJSON tiles found under {export_dir}")
    logger.info("Converting %d CityJSON tiles to IFC", len(cityjson_files))

    args = [(path, config.ignore_duplicate_keys) for path in cityjson_files]
    # maxtasksperchild restarts workers periodically to avoid C-level memory leaks
    with mp.Pool(config.concurrency, maxtasksperchild=5) as pool:
        for _ in pool.imap_unordered(_convert_tile, args):
            pass
