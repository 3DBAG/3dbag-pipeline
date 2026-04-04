from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
import json
from os import getenv
from pathlib import Path
from time import perf_counter
from typing import Any, cast

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
from building_surfaces.walls import shared_walls

from bag3d.common.resources.cjindex import CityIndexResource, open_ready_index
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.utils.cityjsonseq import (
    FeatureRecord,
    write_feature_records_as_cityjsonseq,
)

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
    overhead_s: float
    total_s: float


@dataclass(slots=True)
class BuildingProcessingResult:
    tile_id: str | None
    feature_json: dict[str, Any] | None
    timing: BuildingTiming | None


def _find_building_part_id(feature: dict[str, Any]) -> str | None:
    """Return the BuildingPart object id or fall back to the first CityObject."""
    city_objects = feature.get("CityObjects", {})
    for object_id, city_object in city_objects.items():
        if city_object.get("type") == "BuildingPart":
            return object_id
    return next(iter(city_objects), None)


def _tile_id_from_source_path(
    source_path: str | Path,
    reconstruction_root: Path,
) -> str:
    """Derive the output tile id from an indexed feature source path.

    The reconstruction layout can change from per-feature CityJSONFeature files
    to tile-level NDJSON, so this helper only relies on the path segments under
    the stage root. For the current layout that yields ``z/x/y``; for a tile
    NDJSON file under the same directory tree it yields the same tile id.
    """
    source = Path(source_path)
    try:
        rel_path = source.relative_to(reconstruction_root)
    except ValueError:
        rel_path = source

    if len(rel_path.parts) >= 3:
        return "/".join(rel_path.parts[:3])
    if len(rel_path.parts) == 2:
        return rel_path.parts[0]
    return rel_path.stem


_worker_adjacency: dict[str, list[str]] = {}
_worker_features_index: dict[str, cjindex.FeatureRef] = {}
_worker_dataset_dir: str = ""
_worker_index: cjindex.OpenedIndex | None = None


def _init_worker(
    adjacency: dict[str, list[str]],
    features_index: dict[str, cjindex.FeatureRef],
    dataset_dir: str,
) -> None:
    """Initializer for ProcessPoolExecutor workers.

    Stores the large read-only dicts once per worker process instead of
    pickling them with every submit() call. Opens the index once per worker
    so that per-building calls avoid repeated index-open overhead.
    """
    global _worker_adjacency, _worker_features_index, _worker_dataset_dir, _worker_index
    _worker_adjacency = adjacency
    _worker_features_index = features_index
    _worker_dataset_dir = dataset_dir
    _worker_index = cjindex.OpenedIndex.open(dataset_dir)


def _process_building(
    pand_id: str,
    ref: cjindex.FeatureRef,
    tile_id: str,
    profile: bool,
) -> BuildingProcessingResult:
    """Process a single building: compute shared_walls and inject attributes.

    Returns the modified feature JSON, tile_id, and optional timing information.
    """
    total_start = perf_counter()
    assert _worker_index is not None

    target_load_start = perf_counter()
    target_feature = _worker_index.read_feature_json(ref)
    target_part_id = _find_building_part_id(target_feature)
    load_target_s = perf_counter() - target_load_start
    if target_part_id is None:
        logger.warning(f"No BuildingPart found in {pand_id}, skipping.")
        return BuildingProcessingResult(tile_id=None, feature_json=None, timing=None)

    adjacent_args = []
    adjacent_count = 0
    adjacent_load_start = perf_counter()
    for adj_id in _worker_adjacency.get(pand_id, []):
        adj_ref = _worker_features_index.get(adj_id)
        if adj_ref is None:
            continue
        adj_feature = _worker_index.read_feature_json(adj_ref)
        if adj_feature is None:
            continue
        adj_part_id = _find_building_part_id(adj_feature)
        if adj_part_id is not None:
            adjacent_args.append((adj_feature, adj_part_id))
            adjacent_count += 1
    load_adjacent_s = perf_counter() - adjacent_load_start

    shared_walls_start = perf_counter()
    result = shared_walls(
        target=(target_feature, target_part_id),
        adjacent=adjacent_args,
    )
    shared_walls_s = perf_counter() - shared_walls_start

    for obj in target_feature["CityObjects"].values():
        if obj["type"] == "Building":
            obj.setdefault("attributes", {}).update(
                {
                    "b3_opp_scheidingsmuur": result.area_shared_wall,
                    "b3_opp_buitenmuur": result.area_exterior_wall,
                    "b3_opp_grond": result.area_ground,
                    "b3_opp_dak_plat": result.area_roof_flat,
                    "b3_opp_dak_schuin": result.area_roof_sloped,
                }
            )
            break

    timing = None
    if profile:
        total_s = perf_counter() - total_start
        accounted_s = load_target_s + load_adjacent_s + shared_walls_s
        timing = BuildingTiming(
            pand_id=pand_id,
            adjacent_count=adjacent_count,
            load_target_s=load_target_s,
            load_adjacent_s=load_adjacent_s,
            shared_walls_s=shared_walls_s,
            overhead_s=total_s - accounted_s,
            total_s=total_s,
        )

    return BuildingProcessingResult(
        tile_id=tile_id, feature_json=target_feature, timing=timing
    )


def _summarize_building_timings(
    timings: list[BuildingTiming],
    *,
    adjacency_query_s: float,
    adjacency_rows: int,
    features_written: int,
    tiles_written: int,
    index_load_s: float,
    tile_grouping_s: float,
    processing_total_s: float,
    asset_total_s: float,
    max_workers: int,
) -> dict[str, Any]:
    if not timings:
        return {
            "adjacency_query_s": adjacency_query_s,
            "adjacency_rows": adjacency_rows,
            "features_written": features_written,
            "tiles_written": tiles_written,
            "max_workers": max_workers,
            "index_load_s": index_load_s,
            "tile_grouping_s": tile_grouping_s,
            "processing_total_s": processing_total_s,
            "asset_total_s": asset_total_s,
            "buildings_profiled": 0,
            "load_target_total_s": 0.0,
            "load_adjacent_total_s": 0.0,
            "shared_walls_total_s": 0.0,
            "overhead_total_s": 0.0,
            "mean_building_total_s": 0.0,
            "top_slowest_buildings": [],
        }

    total_building_s = sum(item.total_s for item in timings)
    return {
        "adjacency_query_s": adjacency_query_s,
        "adjacency_rows": adjacency_rows,
        "features_written": features_written,
        "tiles_written": tiles_written,
        "max_workers": max_workers,
        "index_load_s": index_load_s,
        "tile_grouping_s": tile_grouping_s,
        "processing_total_s": processing_total_s,
        "asset_total_s": asset_total_s,
        "buildings_profiled": len(timings),
        "load_target_total_s": sum(item.load_target_s for item in timings),
        "load_adjacent_total_s": sum(item.load_adjacent_s for item in timings),
        "shared_walls_total_s": sum(item.shared_walls_s for item in timings),
        "overhead_total_s": sum(item.overhead_s for item in timings),
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
        AssetKey(["reconstruction", "reconstructed_building_models"]),
    ],
)
def building_surfaces(
    context: AssetExecutionContext,
    config: PartyWallsConfig,
    reconstruction_index: CityIndexResource,
    computation_db: DatabaseResource,
    file_store: FileStoreResource,
) -> list[Path]:
    """Feature-based party walls calculation using bag3d-surfaces shared_walls().

    Pages over reconstructed CityJSONFeature references from ``reconstruction_index``,
    queries adjacent building IDs from the row-based bag_adjacency table, and
    computes party walls using shared_walls(). Results are written to
    stages/party_walls/{tile_id}/.
    """
    asset_start = perf_counter()

    index_load_start = perf_counter()
    idx = open_ready_index(reconstruction_index)
    total = idx.feature_ref_count()
    if total == 0:
        logger.warning("No features found in reconstruction index, skipping.")
        return []

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
    index_load_s = perf_counter() - index_load_start

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

    # Group buildings by tile using the indexed source path, not the per-feature
    # reconstruction file layout.
    tile_grouping_start = perf_counter()
    tiles_with_buildings: dict[str, list[tuple[str, cjindex.FeatureRef]]] = defaultdict(
        list
    )
    for pand_id, ref in features_index.items():
        tile_id = _tile_id_from_source_path(ref.source_path, reconstruction_root)
        tiles_with_buildings[tile_id].append((pand_id, ref))
    tile_grouping_s = perf_counter() - tile_grouping_start

    # Process buildings concurrently; workers return modified feature JSON
    tile_features: dict[str, list[FeatureRecord]] = defaultdict(list)
    building_timings: list[BuildingTiming] = []
    processing_start = perf_counter()
    with ProcessPoolExecutor(
        max_workers=config.concurrency,
        initializer=_init_worker,
        initargs=(
            adjacency,
            features_index,
            reconstruction_index.dataset_dir,
        ),
    ) as executor:
        futures = {}
        for tile_id, buildings in tiles_with_buildings.items():
            for pand_id, ref in buildings:
                future = executor.submit(
                    _process_building,
                    pand_id,
                    ref,
                    tile_id,
                    config.profile,
                )
                futures[future] = (pand_id, ref.source_path)

        for future in futures:
            pand_id, source_path = futures[future]
            try:
                result = future.result()
                if result.feature_json is not None and result.tile_id is not None:
                    tile_features[result.tile_id].append(
                        FeatureRecord(
                            feature=result.feature_json,
                            source_path=source_path,
                        )
                    )
                if result.timing is not None:
                    building_timings.append(result.timing)
            except Exception as exc:
                logger.error(f"Error processing building {pand_id}: {exc}")
    processing_total_s = perf_counter() - processing_start

    # Write per-tile cityjsonseq files
    party_walls_stage_dir = file_store.stage_dir("party_walls")
    files_written: list[Path] = []
    for tile_id, features in tile_features.items():
        parts = tile_id.split("/")
        out_dir = party_walls_stage_dir.joinpath(*parts)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{parts[-1]}.city.jsonl"
        write_feature_records_as_cityjsonseq(out_file, features)
        files_written.append(out_file)

    output_dir = file_store.stage_dir("party_walls")
    metadata: dict[str, Any] = {
        "Nr. features": sum(len(v) for v in tile_features.values()),
        "Nr. tiles": len(files_written),
        "Path": MetadataValue.path(str(output_dir)),
    }
    if config.profile:
        summary = _summarize_building_timings(
            building_timings,
            adjacency_query_s=adjacency_query_s,
            adjacency_rows=len(rows),
            features_written=sum(len(v) for v in tile_features.values()),
            tiles_written=len(files_written),
            index_load_s=index_load_s,
            tile_grouping_s=tile_grouping_s,
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
