"""Local integration for the standalone tools, exposing height-bearing polygons."""

import dagster as dg

from bag3d.underpasses.assets import enrichment


class UnderpassEnrichmentComponent(dg.Component, dg.Model, dg.Resolvable):
    def build_defs(
        self, context: dg.ComponentLoadContext | None = None
    ) -> dg.Definitions:
        return dg.Definitions(
            assets=dg.load_assets_from_modules(
                [enrichment], key_prefix="underpasses", group_name="underpasses"
            )
        )
