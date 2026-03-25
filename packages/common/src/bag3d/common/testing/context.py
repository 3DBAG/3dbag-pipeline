"""Test context builder that validates against the asset definition.

Prevents tests from silently injecting partition_key for non-partitioned assets
(or omitting it for partitioned ones), which would mask wiring bugs that only
surface at runtime.
"""

from contextlib import contextmanager
from typing import Any

from dagster import AssetsDefinition, build_asset_context


@contextmanager
def build_asset_context_for(
    asset_fn: Any,
    *,
    partition_key: str | None = None,
):
    """Build an ``AssetExecutionContext`` that is consistent with *asset_fn*'s definition.

    Raises ``ValueError`` when:
    * ``partition_key`` is supplied but the asset has no ``partitions_def``
    * ``partition_key`` is omitted but the asset declares a ``partitions_def``

    Usage::

        with build_asset_context_for(building_surfaces, partition_key="10/434/716") as ctx:
            result = building_surfaces(ctx, ...)
    """
    if isinstance(asset_fn, AssetsDefinition):
        assets_def = asset_fn
    elif hasattr(asset_fn, "dagster_definition"):
        assets_def = asset_fn.dagster_definition
    else:
        raise TypeError(
            f"Expected a Dagster @asset-decorated function or AssetsDefinition, "
            f"got {type(asset_fn).__name__}"
        )

    has_partitions = assets_def.partitions_def is not None

    if partition_key is not None and not has_partitions:
        raise ValueError(
            f"Asset {assets_def.key} has no partitions_def, but partition_key "
            f"{partition_key!r} was passed to the test context. Either add a "
            f"partitions_def to the asset or stop passing partition_key."
        )
    if partition_key is None and has_partitions:
        raise ValueError(
            f"Asset {assets_def.key} declares partitions_def "
            f"({assets_def.partitions_def}), but no partition_key was passed "
            f"to the test context."
        )

    with build_asset_context(partition_key=partition_key) as ctx:
        yield ctx
