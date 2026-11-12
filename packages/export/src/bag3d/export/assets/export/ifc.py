"""IFC tile export."""

from concurrent.futures import ProcessPoolExecutor
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
) -> None:
    """Tiles for distribution in IFC format.

    Each CityJSON tile is converted to one IFC file per LoD (0, 1.2, 1.3, 2.2).
    The resulting ``.ifc`` files are zipped into a single ``.ifc.zip`` per tile
    by ``compressed_tiles``. Runs after the CityJSON tiles are generated and
    before they are gzipped by ``compressed_tiles``.
    """
    export_dir = file_store.stage_subdir("export", version.version)
    logger.info("IFC export directory: %s (version=%s)", export_dir, version.version)
    cityjson_files = sorted(export_dir.glob("t/**/*.city.json"))
    logger.info("Found %d CityJSON tiles to convert", len(cityjson_files))
    if not cityjson_files:
        tiles_dir = export_dir / "t"
        if tiles_dir.is_dir():
            listing = [str(p.relative_to(export_dir)) for p in tiles_dir.rglob("*")]
            detail = f"Contents of {tiles_dir}: {listing}"
        else:
            detail = f"{tiles_dir} does not exist"
        raise FileNotFoundError(
            f"No CityJSON tiles found under {export_dir} (version={version.version}). "
            f"{detail}"
        )

    args = [(path, config.ignore_duplicate_keys) for path in cityjson_files]
    produced: list[Path] = []
    failed: list[str] = []
    with ProcessPoolExecutor(max_workers=config.concurrency) as executor:
        for cityjson_path, ifc_files in zip(
            cityjson_files, executor.map(_convert_tile, args)
        ):
            if ifc_files:
                produced.extend(ifc_files)
            else:
                failed.append(str(cityjson_path))

    logger.info(
        "Converted %d/%d CityJSON tiles to IFC (%d files)",
        len(cityjson_files) - len(failed),
        len(cityjson_files),
        len(produced),
    )
    if failed:
        raise RuntimeError(
            f"IFC conversion produced no output for {len(failed)} tile(s): {failed}"
        )
