from dagster import Definitions, load_assets_from_modules

from bag3d.common.resources import resource_defs
from bag3d.party_walls.assets import party_walls
from bag3d.party_walls.jobs import job_nl_party_walls, job_nl_party_walls_index

all_assets = load_assets_from_modules(
    modules=(party_walls,), key_prefix="party_walls", group_name="party_walls"
)

defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=[
        job_nl_party_walls,
        job_nl_party_walls_index,
    ],
)
