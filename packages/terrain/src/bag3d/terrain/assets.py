"""Stage assets. Outputs are manifest paths, never raster arrays."""

from pathlib import Path

from bag3d.common.resources.files import FileStoreResource
from dagster import AssetExecutionContext, MetadataValue, Output, asset

from . import downloads, processing, tiles
from ._mapterhorn.export import export
from .settings import TerrainSettings
from .state import file_record, read_manifest, valid_record


def root(file_store: FileStoreResource) -> Path:
    return file_store.stage_dir("terrain")


def result(context: AssetExecutionContext, path: Path) -> Output[Path]:
    manifest = read_manifest(path)
    metadata = {
        "Manifest": MetadataValue.path(str(path)),
        "Files": len(manifest["files"]),
        "Bytes": sum(r["bytes"] for r in manifest["files"]),
    }
    for field in (
        "bounds",
        "remaining_nodata_pixels",
        "fallback_pixels",
        "detailed_tiles",
        "ocean_cells",
        "minzoom",
        "maxzoom",
    ):
        if field in manifest:
            metadata[field] = (
                MetadataValue.json(manifest[field])
                if field == "bounds"
                else manifest[field]
            )
    context.log.info("Completed %s: %s", context.asset_key.to_user_string(), path)
    return Output(path, metadata=metadata)


@asset
def ahn_catalog(
    context: AssetExecutionContext,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Saved paginated AHN5+AHN6 5 m DTM catalog, including interpolation neighbors."""
    return result(
        context,
        downloads.ahn_catalog(
            root(file_store), terrain.bounds, terrain.interpolation_radius_m
        ),
    )


@asset
def ahn_download(
    context: AssetExecutionContext,
    ahn_catalog: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Immutable downloaded AHN rasters and their output extent."""
    return result(
        context, downloads.download_ahn(root(file_store), ahn_catalog, terrain.workers)
    )


@asset
def proj_grids_download(
    context: AssetExecutionContext,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Mandatory NSGI RDTRANS2018/NLGEO2018 and NGA EGM2008 transformation grids."""
    return result(context, downloads.download_grids(root(file_store), terrain.workers))


@asset
def glo30_download(
    context: AssetExecutionContext,
    ahn_download: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Regional GLO-30 input rasters plus catalog-confirmed omitted ocean cells."""
    return result(
        context,
        downloads.download_glo30(root(file_store), ahn_download, terrain.workers),
    )


@asset
def ahn_rasters(
    context: AssetExecutionContext,
    ahn_download: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Separate north-up RD New COGs retaining the original NAP heights."""
    processing.validate_selection(
        ahn_download, terrain.bounds, terrain.interpolation_radius_m
    )
    return result(
        context,
        processing.normalize_ahn(root(file_store), ahn_download, terrain.workers),
    )


@asset
def ahn_filled(
    context: AssetExecutionContext,
    ahn_rasters: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """AHN nodata interpolated with neighbors within a ground-distance radius."""
    processing.validate_selection(
        ahn_rasters, terrain.bounds, terrain.interpolation_radius_m
    )
    return result(
        context,
        processing.fill_ahn(
            root(file_store),
            ahn_rasters,
            terrain.interpolation_radius_m,
            terrain.workers,
        ),
    )


@asset
def ahn_ellipsoid(
    context: AssetExecutionContext,
    ahn_filled: Path,
    proj_grids_download: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Filled AHN converted from NAP to ellipsoidal heights on the native RD grid."""
    return result(
        context,
        processing.convert_ahn(
            root(file_store), ahn_filled, proj_grids_download, terrain.workers
        ),
    )


@asset
def glo30_ellipsoid(
    context: AssetExecutionContext,
    glo30_download: Path,
    proj_grids_download: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Regional GLO-30 and ocean fallback converted from EGM2008 to ellipsoid."""
    return result(
        context,
        processing.convert_glo(
            root(file_store), glo30_download, proj_grids_download, terrain.workers
        ),
    )


@asset
def aggregation_plan(
    context: AssetExecutionContext,
    ahn_ellipsoid: Path,
    glo30_ellipsoid: Path,
    file_store: FileStoreResource,
) -> Output[Path]:
    """Mapterhorn work areas using AHN first and GLO-30 to fill remaining gaps."""
    return result(
        context, tiles.make_plan(root(file_store), ahn_ellipsoid, glo30_ellipsoid)
    )


@asset
def terrain_tiles(
    context: AssetExecutionContext,
    aggregation_plan: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Detailed 512 px lossless Terrarium tiles packed into per-area PMTiles."""
    return result(
        context,
        tiles.aggregate(
            root(file_store), aggregation_plan, terrain.workers, terrain.gdal_bin_dir
        ),
    )


@asset
def terrain_overviews(
    context: AssetExecutionContext,
    terrain_tiles: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Lower zoom levels downsampled by averaging decoded 2×2 height samples."""
    return result(
        context, tiles.overviews(root(file_store), terrain_tiles, terrain.workers)
    )


@asset
def terrain_bundles(
    context: AssetExecutionContext,
    terrain_overviews: Path,
    file_store: FileStoreResource,
) -> Output[Path]:
    """Retained low-zoom and regional PMTiles bundles with terrain metadata."""
    return result(context, tiles.bundle(root(file_store), terrain_overviews))


def export_xyz(store_root: Path, bundles_path: Path, settings: TerrainSettings) -> Path:
    directory, (manifest,) = processing.stage_directory(
        store_root, "terrain_xyz", [bundles_path]
    )
    web_root = directory / "xyz"
    previous_path = directory / "manifest.json"
    if previous_path.exists():
        for record in read_manifest(previous_path)["files"]:
            if not valid_record(record):
                Path(record["path"]).unlink(missing_ok=True)
    export(
        Path(manifest["files"][0]["path"]).parent,
        web_root,
        settings.base_url,
        "Netherlands AHN 5 m terrain",
        tiles.ATTRIBUTION,
        manifest["bounds"],
        settings.workers,
    )
    files = [file_record(p) for p in sorted(web_root.rglob("*.webp"))]
    files.append(file_record(web_root / "tilejson.json"))
    return processing.finish(
        directory,
        files,
        bounds=manifest["bounds"],
        minzoom=manifest["minzoom"],
        maxzoom=manifest["maxzoom"],
        xyz_tiles=len(files) - 1,
    )


@asset
def terrain_xyz(
    context: AssetExecutionContext,
    terrain_bundles: Path,
    file_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Static XYZ lossless WebP pyramid and TileJSON for the 3DBAG viewer."""
    return result(context, export_xyz(root(file_store), terrain_bundles, terrain))
