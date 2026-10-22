import os

from bag3d.common.resources.files import FileStoreResource
import dagster as dg

from bag3d.terrain import assets
from bag3d.terrain.jobs import all_jobs
from bag3d.terrain.settings import TerrainSettings


class TerrainComponent(dg.Component, dg.Model, dg.Resolvable):
    """Netherlands AHN 5 m terrain with ellipsoidal GLO-30 water fallback.

    The graph is static: discovery and downloads occur only during materialization.
    Each processing stage can be selected and rerun independently.
    """

    def build_defs(
        self, context: dg.ComponentLoadContext | None = None
    ) -> dg.Definitions:
        file_store = (
            FileStoreResource(root_dir=os.environ["BAG3D_FILESTORE"])
            if "BAG3D_FILESTORE" in os.environ
            else FileStoreResource.configure_at_launch()
        )
        return dg.Definitions(
            assets=dg.load_assets_from_modules(
                [assets], key_prefix="terrain", group_name="terrain"
            ),
            jobs=all_jobs,
            resources={
                "file_store": file_store,
                "terrain": TerrainSettings.configure_at_launch(),
            },
        )
