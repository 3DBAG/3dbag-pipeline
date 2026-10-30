"""Height-bearing underpass polygons for Roofer reconstruction."""

from os import getenv
from pathlib import Path

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import create_schema, postgrestable_metadata
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Config,
    Output,
    asset,
)
from pydantic import Field

from bag3d.underpasses.enrichment_workflow import run_heights


class HeightsConfig(Config):
    index: str | None = Field(default_factory=lambda: getenv("BAG3D_STREETLIDAR_INDEX"))
    pointcloud_root: str | None = Field(
        default_factory=lambda: getenv("BAG3D_STREETLIDAR_ROOT")
    )
    index_layer: str | None = None
    batch_workers: int = Field(default=16, ge=1)
    pip_workers: int = Field(default=5, ge=1)
    height_workers: int = Field(default=1, ge=1)
    rayon_threads: int = Field(default=3, ge=1)
    grid_cell_size: float = Field(default=500, gt=0, allow_inf_nan=False)
    batch_size: int = Field(default=200, ge=1)
    chunk_size: int = Field(default=1_000_000, ge=1)
    min_points: int = Field(default=1000, ge=0)
    max_points_per_underpass: int = Field(default=10_000_000, ge=1)
    polygon_buffer: float = Field(default=-0.2, allow_inf_nan=False)
    terrain_percentile: float = Field(default=90, ge=0, le=100, allow_inf_nan=False)
    terrain_peak_exclusion_distance: float = Field(default=2, ge=0, allow_inf_nan=False)


@asset(ins={"ahn_filled": AssetIn(key=AssetKey(["terrain", "ahn_filled"]))})
def heights(
    context: AssetExecutionContext,
    config: HeightsConfig,
    computation_db: DatabaseResource,
    extended_geometries: PostgresTableIdentifier,
    ahn_filled: Path,
) -> Output[PostgresTableIdentifier]:
    """Underpass polygons with NAP ceiling estimates and terrain provenance."""
    from underpass_tools.heights import HeightSettings

    create_schema(computation_db, "underpasses", logger=context.log)
    target = PostgresTableIdentifier("underpasses", "heights")
    stats = run_heights(
        computation_db.connection.dsn,
        extended_geometries,
        target,
        ahn_filled,
        HeightSettings(**config.model_dump()),
    )
    return Output(
        target, metadata={**postgrestable_metadata(computation_db, target), **stats}
    )
