"""The standalone job declares cross-location inputs and all local stages."""

from dagster import AssetKey, Definitions

from bag3d.underpasses.code_location import defs


def test_definitions_loadable_and_job_resolvable():
    Definitions.validate_loadable(defs)
    assert defs.get_job_def("underpasses") is not None


def test_asset_dependencies():
    graph = defs.resolve_asset_graph()

    def key(name):
        return AssetKey(["underpasses", name])

    assert graph.get(key("bag_bgt_join")).parent_keys == {
        AssetKey(["bag", "bag_pandactueelbestaand"]),
        AssetKey(["bgt", "bgt_pandactueelbestaand"]),
    }
    assert graph.get(key("bag_minus_bgt")).parent_keys == {key("bag_bgt_join")}
    assert graph.get(key("snapped_differences")).parent_keys == {
        key("bag_bgt_join"),
        key("bag_minus_bgt"),
    }
    assert graph.get(key("geometries")).parent_keys == {key("snapped_differences")}
    assert graph.materializable_asset_keys == {
        key(name)
        for name in (
            "bag_bgt_join",
            "bag_minus_bgt",
            "snapped_differences",
            "geometries",
            "edges",
            "extended_geometries",
            "skipped_underpasses",
        )
    }


def test_edge_dependencies_and_coupled_outputs():
    graph = defs.resolve_asset_graph()
    assert graph.get(AssetKey(["underpasses", "edges"])).parent_keys == {
        AssetKey(["underpasses", "geometries"]),
        AssetKey(["underpasses", "bag_bgt_join"]),
        AssetKey(["intermediary", "bag_adjacency"]),
    }
    for name in ("extended_geometries", "skipped_underpasses"):
        assert graph.get(AssetKey(["underpasses", name])).parent_keys == {
            AssetKey(["underpasses", "edges"]),
            AssetKey(["underpasses", "geometries"]),
        }
