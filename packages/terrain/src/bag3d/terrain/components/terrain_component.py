import os

import dagster as dg

from bag3d.common.resources.files import file_store_from_env

from bag3d.terrain import assets
from bag3d.terrain.jobs import all_jobs, job_terrain_ahn
from bag3d.terrain.settings import TerrainSettings


class TerrainComponent(dg.Component, dg.Model, dg.Resolvable):
    """Netherlands AHN 5 m terrain with ellipsoidal GLO-30 water fallback.

    The graph is static: discovery and downloads occur only during materialization.
    Each processing stage can be selected and rerun independently.
    """

    def build_defs(
        self, context: dg.ComponentLoadContext | None = None
    ) -> dg.Definitions:
        terrain_assets = dg.load_assets_from_modules(
            [assets], key_prefix="terrain", group_name="terrain"
        )
        resources = {
            "terrain_store": file_store_from_env("BAG3D_TERRAIN_DIR", "terrain"),
            "terrain": TerrainSettings.configure_at_launch(),
        }
        jobs = all_jobs
        if os.getenv("BAG3D_INPUT_MODE", "").lower() == "integration_data":
            from bag3d.terrain import fixture_assets

            terrain_assets = [
                definition
                for definition in terrain_assets
                if isinstance(definition, dg.AssetsDefinition)
                and definition.key.path[-1] in ("ahn_rasters", "ahn_filled")
            ]
            terrain_assets.extend(
                definition
                for definition in dg.load_assets_from_modules(
                    [fixture_assets], key_prefix="terrain", group_name="terrain"
                )
                if isinstance(definition, dg.AssetsDefinition)
            )
            resources["integration_data_store"] = file_store_from_env(
                "BAG3D_INTEGRATION_DATA_DIR", "pipeline/integration-data"
            )
            jobs = [job_terrain_ahn]
        return dg.Definitions(assets=terrain_assets, jobs=jobs, resources=resources)
