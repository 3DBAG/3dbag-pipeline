"""Independent Dagster code location for underpass detection."""

from bag3d.common.resources import resource_defs
from dagster import AssetsDefinition, AssetSpec, Definitions, load_assets_from_modules

from bag3d.underpasses.assets import edges, underpasses
from bag3d.underpasses.jobs import job_underpasses

all_assets = [
    definition
    for definition in load_assets_from_modules(
        modules=[underpasses, edges], key_prefix="underpasses", group_name="underpasses"
    )
    if isinstance(definition, AssetsDefinition)
]

defs = Definitions(
    resources=resource_defs,
    assets=[
        *all_assets,
        AssetSpec(["input", "intermediary", "bag_adjacency"]),
        AssetSpec(["bag", "bag_pandactueelbestaand"]),
        AssetSpec(["bgt", "bgt_pandactueelbestaand"]),
    ],
    jobs=[job_underpasses],
)
