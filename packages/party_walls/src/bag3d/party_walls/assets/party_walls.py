from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterable, cast
import json
from os import getenv

from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    get_dagster_logger,
    AssetExecutionContext,
    Config,
)
from pydantic import Field
from psycopg import sql as pgsql
from building_surfaces.walls import shared_walls, write_cityjsonfeature

from bag3d.common.utils.dagster import PartitionDefinition3DBagDistribution
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.common.resources.database import DatabaseResource

logger = get_dagster_logger("party_walls")

# Fallback roofer transform constants (from reconstruction.py)
_ROOFER_TRANSLATE = [171800.0, 472700.0, 0.0]
_ROOFER_SCALE = [0.001, 0.001, 0.001]


class PartyWallsConfig(Config):
    """Configuration for party_walls assets."""

    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_PARTY_WALLS", "4")),
        description="Number of threads for per-tile building processing.",
    )


def visit_directory(z_level: Path) -> Iterable[tuple[str, Path]]:
    if z_level.is_dir():
        for x_level in z_level.iterdir():
            if x_level.is_dir():
                for y_level in x_level.iterdir():
                    if y_level.joinpath("objects").is_dir():
                        for identificatie in y_level.joinpath("objects").iterdir():
                            # cannot use Path methods here, because we have '.' in the file name
                            feature_path = Path(
                                f"{identificatie / 'reconstruct'}/{identificatie.name}.city.jsonl"
                            )
                            if feature_path.exists():
                                yield identificatie.name, feature_path


def features_file_index_generator(
    path_features: Path, max_workers: int = 4
) -> Iterable[tuple[str, Path]]:
    # We are at the root dir of the reconstructed features
    dir_z = [d for d in path_features.iterdir()]
    # Using ThreadPoolExecutor, because a Generator (returned by visit_directory)
    # cannot be pickled, and the ProcessPoolExecutor only accepts pickle-able objects.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for g in executor.map(visit_directory, dir_z):
            for identificatie, path in g:
                yield identificatie, path


@asset(deps=[AssetKey(["reconstruction", "reconstructed_building_models"])])
def features_file_index(
    config: PartyWallsConfig, file_store: FileStoreResource
) -> dict[str, Path]:
    """A mapping of {feature ID: feature file path} for the reconstructed features in
    the geoflow output directory.

    It walks the directory tree, concurrently for each z-level (z/x/y) of the
    reconstructed feature tiles. Parallelization is done with a
    [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor).

    Returns a dict of {feature ID: feature file path}.
    """
    reconstructed_root_dir = file_store.stage_dir("reconstruction")
    return dict(
        features_file_index_generator(reconstructed_root_dir, config.concurrency)
    )


def _read_transform_from_export(
    file_store: FileStoreResource, version: ReleaseVersionResource
) -> dict:
    """Read the CityJSON transform from an existing exported tile file.

    Falls back to hardcoded roofer constants if no export tile is found.
    """
    tile_dir = file_store.stage_dir("export") / version.version / "tiles"
    some_tile = next(tile_dir.rglob("*.city.json"), None) if tile_dir.exists() else None
    if some_tile is not None:
        try:
            transform = json.loads(some_tile.read_text()).get("transform")
            if transform is not None:
                return transform
        except Exception:
            pass
    logger.warning(
        "No export tile found to read transform from; using roofer fallback constants."
    )
    return {
        "scale": _ROOFER_SCALE,
        "translate": _ROOFER_TRANSLATE,
    }


def _load_feature_as_citymodel(path: Path, transform: dict) -> tuple[dict, str | None]:
    """Load a .city.jsonl feature file and wrap it as a minimal CityJSON dict.

    Returns (citymodel_dict, building_part_object_id).
    """
    with path.open(encoding="utf-8") as fh:
        feature = json.load(fh)

    # Wrap as a CityJSON dict with the transform so CityModel can decode vertices
    cm_dict = {
        "type": "CityJSON",
        "version": "1.1",
        "transform": transform,
        "CityObjects": feature.get("CityObjects", {}),
        "vertices": feature.get("vertices", []),
        "_feature": feature,  # keep original for write_cityjsonfeature
    }

    # Find the BuildingPart object_id
    part_id = None
    for obj_id, obj in feature.get("CityObjects", {}).items():
        if obj.get("type") == "BuildingPart":
            part_id = obj_id
            break
    if part_id is None:
        # Fall back to first object if no BuildingPart
        objects = feature.get("CityObjects", {})
        part_id = next(iter(objects)) if objects else None

    return cm_dict, part_id


def _process_building(
    pand_id: str,
    target_path: Path,
    adjacency: dict[str, list[str]],
    features_file_index: dict[str, Path],
    transform: dict,
    output_dir: Path,
) -> Path | None:
    """Process a single building: compute shared_walls and write output.

    Returns the output path, or None if skipped.
    """
    target_cm, target_part_id = _load_feature_as_citymodel(target_path, transform)
    if target_part_id is None:
        logger.warning(f"No BuildingPart found in {pand_id}, skipping.")
        return None

    adjacent_args = []
    for adj_id in adjacency.get(pand_id, []):
        adj_path = features_file_index.get(adj_id)
        if adj_path is None:
            continue
        adj_cm, adj_part_id = _load_feature_as_citymodel(adj_path, transform)
        if adj_part_id is not None:
            adjacent_args.append((adj_cm, adj_part_id))

    result = shared_walls(
        target=(target_cm, target_part_id),
        adjacent=adjacent_args,
    )

    output_path = output_dir / f"{pand_id}.city.jsonl"
    raw_feature = target_cm["_feature"]
    write_cityjsonfeature(raw_feature, result, output_path)
    return output_path


@asset(
    partitions_def=PartitionDefinition3DBagDistribution(),
    pool="party_walls",
    deps=[AssetKey(["input", "intermediary", "bag_adjacency"])],
)
def building_surfaces(
    context: AssetExecutionContext,
    config: PartyWallsConfig,
    features_file_index: dict[str, Path],
    computation_db: DatabaseResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
) -> list[Path]:
    """Feature-based party walls calculation using bag3d-surfaces shared_walls().

    For each building in the tile partition, loads the reconstructed CityJSONFeature,
    queries adjacent building IDs from the row-based bag_adjacency table, and
    computes party walls using shared_walls(). Results are written to
    stages/party_walls/{tile_id}/.
    """
    tile_id = context.partition_key  # e.g. "10/434/716"

    # Filter features_file_index to buildings in this tile
    tile_prefix = str(file_store.stage_dir("reconstruction") / tile_id / "objects" / "")
    tile_features = {
        pand_id: path
        for pand_id, path in features_file_index.items()
        if str(path).startswith(tile_prefix)
    }

    if not tile_features:
        logger.warning(f"No features found for tile {tile_id}, skipping.")
        return []

    transform = _read_transform_from_export(file_store, version)

    # Query bag_adjacency for all pand_ids in this tile
    pand_ids = list(tile_features.keys())
    query = pgsql.SQL(
        """
        SELECT identificatie, adjacent_identificatie
        FROM reconstruction_input.bag_adjacency
        WHERE identificatie = ANY({pand_ids})
        """
    ).format(pand_ids=pgsql.Literal(pand_ids))
    rows = cast(list[dict[str, Any]], computation_db.connection.get_dict(query))
    adjacency: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        adjacency[row["identificatie"]].append(row["adjacent_identificatie"])

    # Prepare output directory
    output_dir = file_store.stage_dir("party_walls") / tile_id
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process buildings concurrently within this tile
    files_written: list[Path] = []
    with ThreadPoolExecutor(max_workers=config.concurrency) as executor:
        futures = {
            executor.submit(
                _process_building,
                pand_id,
                path,
                adjacency,
                features_file_index,
                transform,
                output_dir,
            ): pand_id
            for pand_id, path in tile_features.items()
        }
        for future in futures:
            pand_id = futures[future]
            try:
                result_path = future.result()
                if result_path is not None:
                    files_written.append(result_path)
            except Exception as exc:
                logger.error(f"Error processing building {pand_id}: {exc}")

    context.add_output_metadata(
        metadata={
            "Nr. features": len(files_written),
            "Path": MetadataValue.path(str(output_dir)),
        }
    )
    return files_written
