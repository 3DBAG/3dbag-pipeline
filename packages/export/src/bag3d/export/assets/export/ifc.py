"""IFC tile export."""

from concurrent.futures import ProcessPoolExecutor
from itertools import islice
from os import getenv
from pathlib import Path

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from dagster import AssetKey, Config, MetadataValue, Output, asset, get_dagster_logger
from pydantic import Field

from bag3d.export.ifc.convert import LODS, convert_cityjson_to_ifc

logger = get_dagster_logger("export.ifc")


class IFCConfig(Config):
    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_IFC", "1")),
        description="ProcessPoolExecutor max_workers for IFC conversion",
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
) -> Output[None]:
    """Tiles for distribution in IFC format.

    Each CityJSON tile is converted to one IFC file per LoD (0, 1.2, 1.3, 2.2).
    The resulting ``.ifc`` files are zipped into a single ``.ifc.zip`` per tile
    by ``compressed_tiles``. Runs after the CityJSON tiles are generated and
    before they are gzipped by ``compressed_tiles``.
    """
    export_dir = file_store.stage_subdir("export", version.version)
    tiles_dir = export_dir / "t"
    cityjson_files = sorted(tiles_dir.rglob("*.city.json"))
    if not cityjson_files:
        compressed = next(tiles_dir.rglob("*.city.json.gz"), None)
        detail = (
            f" Found a compressed CityJSON tile at {compressed}." if compressed else ""
        )
        raise FileNotFoundError(
            f"No uncompressed *.city.json tiles under {tiles_dir} for {version.version}. "
            "Run export/reconstruction_output_cityjson before the IFC export."
            f"{detail}"
        )

    logger.info(
        "Converting %d CityJSON tiles under %s to IFC at LoDs %s with %d workers",
        len(cityjson_files),
        tiles_dir,
        ", ".join(LODS),
        config.concurrency,
    )
    args = [(path, config.ignore_duplicate_keys) for path in cityjson_files]
    produced = 0
    failed: list[Path] = []
    with ProcessPoolExecutor(max_workers=config.concurrency) as executor:
        for cityjson_path, ifc_files in zip(
            cityjson_files, executor.map(_convert_tile, args)
        ):
            if ifc_files:
                produced += len(ifc_files)
            else:
                failed.append(cityjson_path)

    if failed:
        sample = ", ".join(str(path) for path in islice(failed, 10))
        raise RuntimeError(
            f"IFC conversion produced no files for {len(failed)} of {len(cityjson_files)} "
            f"tiles under {tiles_dir}. Check the input CityJSON and write permissions. "
            f"Example inputs: {sample}"
        )
    return Output(
        None,
        metadata={
            "artifact path": MetadataValue.path(tiles_dir),
            "CityJSON tiles converted": len(cityjson_files),
            "IFC files written": produced,
            "LoDs": ", ".join(LODS),
        },
    )
