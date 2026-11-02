"""Four independently materializable stages of 2D underpass detection."""

from os import getenv

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import create_schema, postgrestable_metadata
from dagster import AssetExecutionContext, AssetIn, AssetKey, Config, Output, asset
from pydantic import Field

from bag3d.underpasses.workflow import Stage, run_geometry_stage

SCHEMA = "underpasses"


class UnderpassesConfig(Config):
    """Distances are in metres, in the input coordinate system EPSG:28992."""

    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_UNDERPASSES", "4")),
        ge=1,
    )
    chunk_size: int = Field(default=1000, ge=1)
    snap_tolerance: float = Field(default=0.01, ge=0, allow_inf_nan=False)
    buffer_distance: float = Field(default=0.2, ge=0, allow_inf_nan=False)


def _geometry_output(
    context: AssetExecutionContext,
    config: UnderpassesConfig,
    computation_db: DatabaseResource,
    stage: Stage,
    source: PostgresTableIdentifier,
    joined: PostgresTableIdentifier | None = None,
) -> Output[PostgresTableIdentifier]:
    create_schema(computation_db, SCHEMA, logger=context.log)
    target = PostgresTableIdentifier(SCHEMA, stage)
    stats = run_geometry_stage(
        computation_db.connection.dsn,
        stage,
        source,
        target,
        concurrency=config.concurrency,
        chunk_size=config.chunk_size,
        buffer_distance=config.buffer_distance,
        snap_tolerance=config.snap_tolerance,
        bag_bgt_join=joined,
    )
    context.log.info(
        f"{stage}: processed {stats['processed']} buildings, wrote {stats['written']} rows"
    )
    return Output(
        target,
        metadata={**postgrestable_metadata(computation_db, target), **stats},
    )


@asset(
    ins={
        "bag_bgt_join": AssetIn(key=AssetKey(["input", "intermediary", "bag_bgt_join"]))
    },
    op_tags={"compute_kind": "python"},
)
def bag_minus_bgt(
    context: AssetExecutionContext,
    config: UnderpassesConfig,
    computation_db: DatabaseResource,
    bag_bgt_join: PostgresTableIdentifier,
) -> Output[PostgresTableIdentifier]:
    """BAG minus BGT differences passing the initial double-buffer test."""
    return _geometry_output(
        context, config, computation_db, "bag_minus_bgt", bag_bgt_join
    )


@asset(
    ins={
        "bag_minus_bgt": AssetIn(key=AssetKey(["underpasses", "bag_minus_bgt"])),
        "bag_bgt_join": AssetIn(
            key=AssetKey(["input", "intermediary", "bag_bgt_join"])
        ),
    },
    op_tags={"compute_kind": "python"},
)
def snapped_differences(
    context: AssetExecutionContext,
    config: UnderpassesConfig,
    computation_db: DatabaseResource,
    bag_minus_bgt: PostgresTableIdentifier,
    bag_bgt_join: PostgresTableIdentifier,
) -> Output[PostgresTableIdentifier]:
    """Intersection of differences after snapping BAG to BGT and BGT to BAG."""
    return _geometry_output(
        context,
        config,
        computation_db,
        "snapped_differences",
        bag_minus_bgt,
        bag_bgt_join,
    )


@asset(op_tags={"compute_kind": "python"})
def geometries(
    context: AssetExecutionContext,
    config: UnderpassesConfig,
    computation_db: DatabaseResource,
    snapped_differences: PostgresTableIdentifier,
) -> Output[PostgresTableIdentifier]:
    """Final filtered underpass polygons with deterministic, run-local integer IDs."""
    return _geometry_output(
        context, config, computation_db, "geometries", snapped_differences
    )
