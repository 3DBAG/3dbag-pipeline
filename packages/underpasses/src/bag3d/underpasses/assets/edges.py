"""Classify detected underpass boundaries and offset their movable edges."""

from os import getenv

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import create_schema, postgrestable_metadata
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetOut,
    Config,
    Output,
    asset,
    multi_asset,
)
from pydantic import Field

from bag3d.underpasses.assets import underpasses
from bag3d.underpasses.edge_workflow import (
    classification_query,
    offset_query,
    run_edges,
)


class EdgeExecutionConfig(Config):
    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_UNDERPASSES", "4")),
        ge=1,
    )
    chunk_size: int = Field(default=1000, ge=1)


class EdgeClassificationConfig(EdgeExecutionConfig):
    grid_size: float = Field(default=0.001, gt=0, allow_inf_nan=False)
    snap_tolerance: float = Field(default=0.03, ge=0, allow_inf_nan=False)


class EdgeOffsetConfig(EdgeExecutionConfig):
    offset_distance: float = Field(default=0.05, allow_inf_nan=False)
    tolerance: float = Field(default=0.001, gt=0, allow_inf_nan=False)


@asset(ins={"bag_adjacency": AssetIn(key=["intermediary", "bag_adjacency"])})
def edges(
    context: AssetExecutionContext,
    config: EdgeClassificationConfig,
    computation_db: DatabaseResource,
    geometries: PostgresTableIdentifier,
    bag_bgt_join: PostgresTableIdentifier,
    bag_adjacency: PostgresTableIdentifier,
) -> Output[PostgresTableIdentifier]:
    """Interior, exterior and shared linework, using adjacent BGT footprints."""
    create_schema(computation_db, underpasses.SCHEMA, logger=context.log)
    target = PostgresTableIdentifier(underpasses.SCHEMA, "edges")
    stats = run_edges(
        computation_db.connection.dsn,
        classification_query(geometries, bag_bgt_join, bag_adjacency),
        target,
        concurrency=config.concurrency,
        chunk_size=config.chunk_size,
        settings={
            "grid_size": config.grid_size,
            "snap_tolerance": config.snap_tolerance,
        },
    )
    context.log.info(f"Edge classification: {stats}")
    return Output(
        target, metadata={**postgrestable_metadata(computation_db, target), **stats}
    )


@multi_asset(
    outs={
        "extended_geometries": AssetOut(dagster_type=PostgresTableIdentifier),
        "skipped_underpasses": AssetOut(dagster_type=PostgresTableIdentifier),
    }
)
def offset_edges(
    context: AssetExecutionContext,
    config: EdgeOffsetConfig,
    computation_db: DatabaseResource,
    edges: PostgresTableIdentifier,
    geometries: PostgresTableIdentifier,
):
    """Offset exterior boundaries and atomically record successes and known exclusions."""
    create_schema(computation_db, underpasses.SCHEMA, logger=context.log)
    target = PostgresTableIdentifier(underpasses.SCHEMA, "extended_geometries")
    skipped = PostgresTableIdentifier(underpasses.SCHEMA, "skipped_underpasses")
    stats = run_edges(
        computation_db.connection.dsn,
        offset_query(geometries, edges),
        target,
        skipped=skipped,
        concurrency=config.concurrency,
        chunk_size=config.chunk_size,
        settings={
            "offset_distance": config.offset_distance,
            "tolerance": config.tolerance,
        },
    )
    context.log.info(f"Edge offset: {stats}")
    yield Output(
        target,
        output_name="extended_geometries",
        metadata={**postgrestable_metadata(computation_db, target), **stats},
    )
    yield Output(
        skipped,
        output_name="skipped_underpasses",
        metadata={**postgrestable_metadata(computation_db, skipped), **stats},
    )
