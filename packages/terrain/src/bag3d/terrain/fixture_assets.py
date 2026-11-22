"""Read-only raw AHN fixture acquisition feeding the ordinary offline stages."""

from pathlib import Path

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.terrain import contains, snapshot_path, terrain_snapshot
from dagster import AssetExecutionContext, Output, asset

from .assets import result
from .processing import validate_selection
from .settings import TerrainSettings
from .state import fingerprint, read_manifest, save_manifest, verify_inputs


@asset
def ahn_catalog(
    context: AssetExecutionContext,
    terrain_store: FileStoreResource,
    integration_data_store: FileStoreResource,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Verify the portable raw AHN catalog and resolve its declared paths."""
    snapshot_root = Path(integration_data_store.root_dir)
    catalog = terrain_snapshot(snapshot_root)
    bounds = terrain.bounds or catalog["bounds"]
    if not contains(catalog["bounds"], bounds):
        raise ValueError("Requested terrain bounds exceed the integration snapshot AOI")
    if terrain.interpolation_radius_m > catalog["selection_radius_m"]:
        raise ValueError("Requested interpolation radius exceeds the terrain snapshot")
    files = []
    for record in catalog["files"]:
        resolved = {**record, "path": str(snapshot_path(snapshot_root, record["path"]))}
        if not resolved.get("url"):
            resolved.pop("url", None)
        files.append(resolved)
    return result(
        context,
        save_manifest(
            terrain_store.path
            / "catalogs"
            / f"fixture-{fingerprint([catalog, files, terrain.bounds])}.json",
            files,
            bounds=bounds,
            requested_bounds=terrain.bounds,
            selection_radius_m=catalog["selection_radius_m"],
            source_manifest_sha256=catalog["source_manifest_sha256"],
            fixture=True,
        ),
    )


@asset
def ahn_download(
    context: AssetExecutionContext,
    ahn_catalog: Path,
    terrain: TerrainSettings,
) -> Output[Path]:
    """Validate the resolved raw inputs before offline processing."""
    manifest = read_manifest(ahn_catalog)
    validate_selection(ahn_catalog, terrain.bounds, terrain.interpolation_radius_m)
    verify_inputs(manifest)
    return result(context, ahn_catalog)
