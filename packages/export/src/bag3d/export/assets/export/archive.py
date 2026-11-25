import csv
import gzip
import zipfile
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from os import getenv
from pathlib import Path
from shutil import copyfileobj
from zipfile import ZipFile

from bag3d.common.resources.executables import CommandRunner, GDALResource
from bag3d.common.resources.files import FileStoreResource, TilesDirResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.common.utils.files import tile_basename, tile_sidecars
from dagster import (
    AssetIn,
    AssetKey,
    Config,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
)
from pydantic import Field

from bag3d.export.assets.export.metadata import read_indexed_tile_ids

logger = get_dagster_logger("export.archive")


def _run_geopackage_command(
    runner: CommandRunner, executable: str, command: list[str], operation: str
) -> None:
    try:
        result = runner.run(" ".join(command), exe_name=executable, logger=logger)
    except Exception as exc:
        raise RuntimeError(f"Could not run {executable} to {operation}. {exc}") from exc
    if not result.success:
        detail = (
            result.stderr.strip() or result.stdout.strip() or "no diagnostic output"
        )
        raise RuntimeError(
            f"{executable} could not {operation} (exit {result.returncode}). {detail[-1000:]}"
        )


@asset(
    ins={"merged_quadtree": AssetIn(key=AssetKey(("export", "merged_quadtree")))},
)
def geopackage(
    file_store: FileStoreResource,
    gdal: GDALResource,
    version: ReleaseVersionResource,
    merged_quadtree: Path,
    tiles_dir: TilesDirResource,
) -> Output[Path]:
    """GeoPackage of the whole Netherlands, containing all 3D BAG layers."""
    path_export_dir = file_store.stage_subdir("export", version.version)
    path_tiles_dir = path_export_dir / tiles_dir.name
    path_nl = path_export_dir.joinpath("3dbag_nl.gpkg")

    # Remove existing
    path_nl.unlink(missing_ok=True)

    with merged_quadtree.open("r") as fo:
        csvreader = csv.DictReader(fo, delimiter="\t")
        missing = {"id", "nr_items", "leaf"}.difference(csvreader.fieldnames or ())
        if missing:
            raise ValueError(
                f"Quadtree {merged_quadtree} is missing columns {sorted(missing)}. "
                "Check export/merged_quadtree before building the national GeoPackage."
            )
        try:
            leaf_ids = [
                row["id"]
                for row in csvreader
                if row["leaf"] == "true" and int(row["nr_items"]) > 0
            ]
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid nr_items in {merged_quadtree} at line {csvreader.line_num}. "
                "Check export/merged_quadtree."
            ) from exc
    logger.info(
        "Building %s from %d nonempty leaves in %s using tiles under %s",
        path_nl,
        len(leaf_ids),
        merged_quadtree,
        path_tiles_dir,
    )
    # Find the first leaf that actually has exported data
    first_i_with_data = None
    for i, lid in enumerate(leaf_ids):
        path_first = tile_basename(path_tiles_dir, lid).with_suffix(".gpkg")
        if path_first.is_file():
            first_i_with_data = i
            break
    if first_i_with_data is None:
        raise ValueError(
            f"Did not find any .gpkg file under {path_tiles_dir} for the "
            f"{len(leaf_ids)} nonempty leaves in {merged_quadtree}. "
            "Check export/reconstruction_output_gpkg and verify the tile paths "
            "before running compression."
        )

    # Init the gpkg-s so that we can append to them later
    lid = leaf_ids[first_i_with_data]
    first_path_with_data = tile_basename(path_tiles_dir, lid).with_suffix(".gpkg")
    cmd = [
        "OGR_SQLITE_SYNCHRONOUS=OFF",
        "{exe}",
        "-gt",
        "65536",
        "-lco",
        "SPATIAL_INDEX=NO",
        "-f",
        "GPKG",
        str(path_nl),
        str(first_path_with_data),
    ]
    runner = gdal.runner
    _run_geopackage_command(
        runner, "ogr2ogr", cmd, f"initialize {path_nl} from {first_path_with_data}"
    )

    nr_failed = first_i_with_data
    failed_sample = leaf_ids[: min(first_i_with_data, 10)]
    failure_details = [
        f"{lid}: {tile_basename(path_tiles_dir, lid).with_suffix('.gpkg')} was missing"
        for lid in failed_sample
    ]
    for lid in leaf_ids[first_i_with_data + 1 :]:
        path_with_data = tile_basename(path_tiles_dir, lid).with_suffix(".gpkg")
        cmd = [
            "OGR_SQLITE_SYNCHRONOUS=OFF",
            "{exe}",
            "-gt",
            "65536",
            "-lco",
            "SPATIAL_INDEX=NO",
            "-append",
            "-f",
            "GPKG",
            str(path_nl),
            str(path_with_data),
        ]
        try:
            _run_geopackage_command(
                runner,
                "ogr2ogr",
                cmd,
                f"append tile {lid} ({path_with_data}) to {path_nl}",
            )
        except RuntimeError as exc:
            nr_failed += 1
            if len(failed_sample) < 10:
                failed_sample.append(lid)
                failure_details.append(str(exc)[:500])

    if nr_failed:
        logger.warning(
            "%s is missing %d of %d GeoPackage tiles. Check "
            "export/reconstruction_output_gpkg and the ogr2ogr logs before publishing. "
            "Up to 10 failures: %s",
            path_nl,
            nr_failed,
            len(leaf_ids),
            " | ".join(failure_details),
        )

    layers = [
        "pand",
        "lod12_2d",
        "lod12_3d",
        "lod13_2d",
        "lod13_3d",
        "lod22_2d",
        "lod22_3d",
    ]
    for name_layer in layers:
        cmd = [
            "OGR_SQLITE_SYNCHRONOUS=OFF",
            "{exe}",
            "-sql",
            f"\"SELECT CreateSpatialIndex('{name_layer}','geom')\"",
            str(path_nl),
        ]
        _run_geopackage_command(
            runner,
            "ogrinfo",
            cmd,
            f"create the spatial index for {name_layer} in {path_nl}",
        )

    path_nl_zip = path_nl.with_suffix(".gpkg.zip")
    # Remove existing
    path_nl_zip.unlink(missing_ok=True)
    cmd = ["{exe}", "--junk-paths", str(path_nl_zip), str(path_nl)]
    _run_geopackage_command(
        runner, "sozip", cmd, f"compress {path_nl} into {path_nl_zip}"
    )

    metadata = {
        "artifact path": MetadataValue.path(path_nl_zip),
        "quadtree": MetadataValue.path(merged_quadtree),
        "expected tiles": len(leaf_ids),
        "nr_failed": nr_failed,
        "sample of failed tile IDs": MetadataValue.json(failed_sample),
    }
    metadata["size uncompressed [Gb]"] = path_nl.stat().st_size * 1e-9
    metadata["size compressed [Gb]"] = path_nl_zip.stat().st_size * 1e-9
    path_nl.unlink(missing_ok=True)

    return Output(path_nl_zip, metadata=metadata)


def compress_files(input_tile_path: tuple[str, Path]) -> dict[str, Path]:
    tile_id, tiles_dir = input_tile_path
    try:
        return _compress_files(tile_id, tiles_dir)
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        raise RuntimeError(
            f"Could not compress tile {tile_id} under {tiles_dir}. "
            "Check the source files and output archives for this tile."
        ) from exc


def _compress_files(tile_id: str, tiles_dir: Path) -> dict[str, Path]:
    basename = tile_basename(tiles_dir, tile_id)
    missing = {}

    # OBJ
    obj_zip = basename.with_name(f"{basename.name}-obj.zip")
    obj_files = tile_sidecars(basename, ".obj", ".mtl")
    if not obj_files and not obj_zip.exists():
        missing["OBJ"] = basename
    with ZipFile(
        file=obj_zip, mode="a", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as oz:
        for f in obj_files:
            oz.write(filename=f, arcname=f.name)
            f.unlink()

    # CityJSON
    cj_file = basename.with_suffix(".city.json")
    cj_zip = cj_file.with_suffix(".json.gz")
    if cj_file.exists():
        with cj_file.open("rb") as f_in, gzip.open(cj_zip, "wb") as f_out:
            copyfileobj(f_in, f_out)
        cj_file.unlink()
    elif not cj_zip.exists():
        missing["CityJSON"] = cj_file

    # GPKG
    gpkg_file = basename.with_suffix(".gpkg")
    gpkg_zip = gpkg_file.with_suffix(".gpkg.gz")
    if gpkg_file.exists():
        with gpkg_file.open("rb") as f_in, gzip.open(gpkg_zip, "wb") as f_out:
            copyfileobj(f_in, f_out)
        gpkg_file.unlink()
    elif not gpkg_zip.exists():
        missing["GeoPackage"] = gpkg_file

    # IFC
    ifc_zip = basename.with_name(f"{basename.name}-ifc.zip")
    ifc_files = tile_sidecars(basename, ".ifc")
    if not ifc_files and not ifc_zip.exists():
        missing["IFC"] = basename
    with ZipFile(
        file=ifc_zip, mode="a", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as iz:
        for f in ifc_files:
            iz.write(filename=f, arcname=f.name)
            f.unlink()
    return missing


class CompressionConfig(Config):
    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_ARCHIVE", "1")),
        description="ProcessPoolExecutor max_workers for compression",
    )


@asset(
    deps={
        AssetKey(("export", "geopackage")),
        AssetKey(("export", "reconstruction_output_ifc")),
    },
    pool="compression",
)
def compressed_tiles(
    config: CompressionConfig,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    export_index: Path,
    tiles_dir: TilesDirResource,
) -> Output[None]:
    """Each format is gzipped individually in each tile, for better transfer over the
    web. The OBJ files are collected into a single .zip file."""
    path_export_dir = file_store.stage_subdir("export", version.version)
    path_tiles_dir = path_export_dir / tiles_dir.name
    tile_ids = read_indexed_tile_ids(export_index)

    logger.info(
        "Compressing %d indexed tiles from %s under %s with %d workers",
        len(tile_ids),
        export_index,
        path_tiles_dir,
        config.concurrency,
    )
    missing_counts: Counter[str] = Counter()
    first_missing = {}
    with ProcessPoolExecutor(max_workers=config.concurrency) as executor:
        compressed = executor.map(
            compress_files, ((tile_id, path_tiles_dir) for tile_id in tile_ids)
        )
        for tile_id, missing in zip(tile_ids, compressed):
            missing_counts.update(missing.keys())
            for data_format, path in missing.items():
                first_missing.setdefault(data_format, (tile_id, str(path)))
    for data_format, count in missing_counts.items():
        sample_id, sample_path = first_missing[data_format]
        logger.warning(
            "No %s input or existing archive for %d tiles. Example tile %s "
            "(expected input path or filename prefix %s). Check that the upstream %s export ran "
            "for this release and produced files for those tiles.",
            data_format,
            count,
            sample_id,
            sample_path,
            data_format,
        )
    if not tile_ids:
        logger.warning(
            "No tiles to compress. Check the tile_id rows in %s.", export_index
        )
    return Output(
        None,
        metadata={
            "artifact path": MetadataValue.path(path_tiles_dir),
            "export index": MetadataValue.path(export_index),
            "tiles processed": len(tile_ids),
            "missing inputs by format": MetadataValue.json(dict(missing_counts)),
        },
    )
