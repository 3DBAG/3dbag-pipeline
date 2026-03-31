from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from typing import Any, cast
import json
from os import getenv
from time import perf_counter

import cjindex
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

from bag3d.common.resources import NlTransform
from bag3d.common.resources.cjindex import CityIndexResource, open_ready_index
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.database import DatabaseResource

logger = get_dagster_logger("party_walls")

_PAGE_SIZE = 1000


def _env_flag(name: str) -> bool:
    return getenv(name, "0").strip().lower() in {"1", "true", "yes", "on"}


class PartyWallsConfig(Config):
    """Configuration for party_walls assets."""

    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_PARTY_WALLS", "4")),
        description="Number of threads for building processing.",
    )
    profile: bool = Field(
        default_factory=lambda: _env_flag("BAG3D_PROFILE_BUILDING_SURFACES"),
        description="Write a minimal timing summary for building_surfaces().",
    )


@dataclass(slots=True)
class BuildingTiming:
    pand_id: str
    adjacent_count: int
    load_target_s: float
    load_adjacent_s: float
    shared_walls_s: float
    write_output_s: float
    total_s: float


@dataclass(slots=True)
class BuildingProcessingResult:
    output_path: Path | None
    timing: BuildingTiming | None


@asset(deps=[AssetKey(["reconstruction", "reconstructed_building_models"])])
def features_file_index(
    reconstruction_index: CityIndexResource,
) -> dict:
    """Index-readiness asset for reconstructed features.

    Opens the ``reconstruction_index`` (creating or refreshing the SQLite
    index if needed) and returns a small status payload.  The asset key is
    preserved so that downstream jobs and the ``building_surfaces`` asset
    continue to resolve against it.
    """
    idx = open_ready_index(reconstruction_index)
    count = idx.feature_ref_count()
    logger.info(f"Reconstruction index ready: {count} features indexed.")
    return {"indexed_feature_count": count}


def _load_feature_as_citymodel(feature: dict, transform: dict) -> tuple[dict, str | None]:
    """Wrap a CityJSONFeature dict as a minimal CityJSON dict.

    Returns (citymodel_dict, building_part_object_id).
    """
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
        objects = feature.get("CityObjects", {})
        part_id = next(iter(objects)) if objects else None

    return cm_dict, part_id


_worker_adjacency: dict[str, list[str]] = {}
_worker_features_index: dict[str, cjindex.FeatureRef] = {}
_worker_transform: dict = {}
_worker_dataset_dir: str = ""


def _init_worker(
    adjacency: dict[str, list[str]],
    features_index: dict[str, cjindex.FeatureRef],
    transform: dict,
    dataset_dir: str,
) -> None:
    """Initializer for ProcessPoolExecutor workers.

    Stores the large read-only dicts once per worker process instead of
    pickling them with every submit() call.
    """
    global _worker_adjacency, _worker_features_index, _worker_transform, _worker_dataset_dir
    _worker_adjacency = adjacency
    _worker_features_index = features_index
    _worker_transform = transform
    _worker_dataset_dir = dataset_dir


def _process_building(
    pand_id: str,
    ref: cjindex.FeatureRef,
    output_dir: Path,
    profile: bool,
) -> BuildingProcessingResult:
    """Process a single building: compute shared_walls and write output.

    Returns the output path and optional timing information.
    """
    total_start = perf_counter()

    # Open per-worker index instance
    worker_idx = cjindex.OpenedIndex.open(_worker_dataset_dir)

    target_load_start = perf_counter()
    target_bytes = worker_idx.read_feature_bytes(ref)
    target_feature = json.loads(target_bytes)
    target_cm, target_part_id = _load_feature_as_citymodel(
        target_feature, _worker_transform
    )
    load_target_s = perf_counter() - target_load_start
    if target_part_id is None:
        logger.warning(f"No BuildingPart found in {pand_id}, skipping.")
        return BuildingProcessingResult(output_path=None, timing=None)

    adjacent_args = []
    adjacent_count = 0
    adjacent_load_start = perf_counter()
    for adj_id in _worker_adjacency.get(pand_id, []):
        if _worker_features_index.get(adj_id) is None:
            continue
        adj_bytes = worker_idx.get_bytes(adj_id)
        if adj_bytes is None:
            continue
        adj_feature = json.loads(adj_bytes)
        adj_cm, adj_part_id = _load_feature_as_citymodel(adj_feature, _worker_transform)
        if adj_part_id is not None:
            adjacent_args.append((adj_cm, adj_part_id))
            adjacent_count += 1
    load_adjacent_s = perf_counter() - adjacent_load_start

    shared_walls_start = perf_counter()
    result = shared_walls(
        target=(target_cm, target_part_id),
        adjacent=adjacent_args,
    )
    shared_walls_s = perf_counter() - shared_walls_start

    output_path = output_dir / f"{pand_id}.city.jsonl"
    raw_feature = target_cm["_feature"]
    write_start = perf_counter()
    write_cityjsonfeature(raw_feature, result, output_path)
    write_output_s = perf_counter() - write_start

    timing = None
    if profile:
        timing = BuildingTiming(
            pand_id=pand_id,
            adjacent_count=adjacent_count,
            load_target_s=load_target_s,
            load_adjacent_s=load_adjacent_s,
            shared_walls_s=shared_walls_s,
            write_output_s=write_output_s,
            total_s=perf_counter() - total_start,
        )

    return BuildingProcessingResult(output_path=output_path, timing=timing)


def _summarize_building_timings(
    timings: list[BuildingTiming],
    *,
    adjacency_query_s: float,
    adjacency_rows: int,
    files_written: int,
    processing_total_s: float,
    asset_total_s: float,
    max_workers: int,
) -> dict[str, Any]:
    if not timings:
        return {
            "adjacency_query_s": adjacency_query_s,
            "adjacency_rows": adjacency_rows,
            "files_written": files_written,
            "max_workers": max_workers,
            "processing_total_s": processing_total_s,
            "asset_total_s": asset_total_s,
            "buildings_profiled": 0,
            "load_target_total_s": 0.0,
            "load_adjacent_total_s": 0.0,
            "shared_walls_total_s": 0.0,
            "write_output_total_s": 0.0,
            "mean_building_total_s": 0.0,
            "top_slowest_buildings": [],
        }

    total_building_s = sum(item.total_s for item in timings)
    return {
        "adjacency_query_s": adjacency_query_s,
        "adjacency_rows": adjacency_rows,
        "files_written": files_written,
        "max_workers": max_workers,
        "processing_total_s": processing_total_s,
        "asset_total_s": asset_total_s,
        "buildings_profiled": len(timings),
        "load_target_total_s": sum(item.load_target_s for item in timings),
        "load_adjacent_total_s": sum(item.load_adjacent_s for item in timings),
        "shared_walls_total_s": sum(item.shared_walls_s for item in timings),
        "write_output_total_s": sum(item.write_output_s for item in timings),
        "mean_building_total_s": total_building_s / len(timings),
        "top_slowest_buildings": [
            asdict(item)
            for item in sorted(timings, key=lambda item: item.total_s, reverse=True)[
                :10
            ]
        ],
    }


@asset(
    deps=[
        AssetKey(["input", "intermediary", "bag_adjacency"]),
        AssetKey(["party_walls", "features_file_index"]),
    ],
)
def building_surfaces(
    context: AssetExecutionContext,
    config: PartyWallsConfig,
    reconstruction_index: CityIndexResource,
    computation_db: DatabaseResource,
    file_store: FileStoreResource,
    nl_transform: NlTransform,
) -> list[Path]:
    """Feature-based party walls calculation using bag3d-surfaces shared_walls().

    Pages over reconstructed CityJSONFeature references from ``reconstruction_index``,
    queries adjacent building IDs from the row-based bag_adjacency table, and
    computes party walls using shared_walls(). Results are written to
    stages/party_walls/{tile_id}/.
    """
    idx = open_ready_index(reconstruction_index)
    total = idx.feature_ref_count()
    if total == 0:
        logger.warning("No features found in reconstruction index, skipping.")
        return []

    asset_start = perf_counter()
    transform = {"translate": nl_transform.translate, "scale": nl_transform.scale}
    reconstruction_root = file_store.stage_dir("reconstruction")

    # Collect all feature refs
    features_index: dict[str, cjindex.FeatureRef] = {}
    offset = 0
    while offset < total:
        refs = idx.feature_ref_page(offset, _PAGE_SIZE)
        if not refs:
            break
        for ref in refs:
            features_index[ref.feature_id] = ref
        offset += len(refs)

    pand_ids = list(features_index.keys())

    # Query bag_adjacency for all pand_ids
    adjacency_query_start = perf_counter()
    query = pgsql.SQL(
        """
        SELECT identificatie, adjacent_identificatie
        FROM reconstruction_input.bag_adjacency
        WHERE identificatie = ANY({pand_ids})
        """
    ).format(pand_ids=pgsql.Literal(pand_ids))
    rows = cast(list[dict[str, Any]], computation_db.connection.get_dict(query))
    adjacency_query_s = perf_counter() - adjacency_query_start
    adjacency: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        adjacency[row["identificatie"]].append(row["adjacent_identificatie"])

    # Group buildings by tile and create output directories
    tiles_with_buildings: dict[str, list[tuple[str, cjindex.FeatureRef]]] = defaultdict(list)
    for pand_id, ref in features_index.items():
        # Extract tile_id from source_path: .../reconstruction/{z}/{x}/{y}/objects/{pand_id}/...
        source = Path(ref.source_path)
        try:
            rel_path = source.relative_to(reconstruction_root)
            tile_id = "/".join(rel_path.parts[:3])
        except ValueError:
            tile_id = pand_id
        tiles_with_buildings[tile_id].append((pand_id, ref))

    # Process buildings concurrently
    files_written: list[Path] = []
    building_timings: list[BuildingTiming] = []
    processing_start = perf_counter()
    with ProcessPoolExecutor(
        max_workers=config.concurrency,
        initializer=_init_worker,
        initargs=(adjacency, features_index, transform, reconstruction_index.dataset_dir),
    ) as executor:
        futures = {}
        for tile_id, buildings in tiles_with_buildings.items():
            output_dir = file_store.stage_dir("party_walls") / tile_id
            output_dir.mkdir(parents=True, exist_ok=True)

            for pand_id, ref in buildings:
                future = executor.submit(
                    _process_building,
                    pand_id,
                    ref,
                    output_dir,
                    config.profile,
                )
                futures[future] = pand_id

        for future in futures:
            pand_id = futures[future]
            try:
                result = future.result()
                if result.output_path is not None:
                    files_written.append(result.output_path)
                if result.timing is not None:
                    building_timings.append(result.timing)
            except Exception as exc:
                logger.error(f"Error processing building {pand_id}: {exc}")
    processing_total_s = perf_counter() - processing_start

    output_dir = file_store.stage_dir("party_walls")
    metadata: dict[str, Any] = {
        "Nr. features": len(files_written),
        "Path": MetadataValue.path(str(output_dir)),
    }
    if config.profile:
        summary = _summarize_building_timings(
            building_timings,
            adjacency_query_s=adjacency_query_s,
            adjacency_rows=len(rows),
            files_written=len(files_written),
            processing_total_s=processing_total_s,
            asset_total_s=perf_counter() - asset_start,
            max_workers=config.concurrency,
        )
        profile_path = output_dir / "_profiling" / "building_surfaces_profile.json"
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        profile_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
        )
        metadata["Profile"] = MetadataValue.path(str(profile_path))
        metadata["Profile shared_walls total (s)"] = round(
            summary["shared_walls_total_s"], 3
        )
        metadata["Profile asset total (s)"] = round(summary["asset_total_s"], 3)
    context.add_output_metadata(metadata=metadata)
    return files_written
