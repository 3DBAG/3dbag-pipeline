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
        AssetKey(["input", "intermediary", "bag_adjacency"]),
    }
    for name in ("extended_geometries", "skipped_underpasses"):
        assert graph.get(AssetKey(["underpasses", name])).parent_keys == {
            AssetKey(["underpasses", "edges"]),
            AssetKey(["underpasses", "geometries"]),
        }


def test_adjacency_input_matches_core_package_registration(monkeypatch):
    """Exercise core's real asset with its package prefix, not a mock source."""
    import importlib
    from pathlib import Path

    import pytest
    from dagster import AssetsDefinition, load_assets_from_package_module

    core_src = Path(__file__).resolve().parents[2] / "core" / "src"
    if not core_src.is_dir():
        pytest.skip("Cross-package contract check requires the repository checkout")
    monkeypatch.syspath_prepend(str(core_src))
    package = importlib.import_module("bag3d.core.assets.input")
    # Match the input package registration in core.asset_groups.
    registered = load_assets_from_package_module(package, key_prefix="input")
    adjacency = next(
        key
        for definition in registered
        if isinstance(definition, AssetsDefinition)
        for key in definition.keys
        if key.path[-1] == "bag_adjacency"
    )
    graph = defs.resolve_asset_graph()
    assert adjacency in graph.get(AssetKey(["underpasses", "edges"])).parent_keys
    assert graph.get(adjacency).is_materializable is False
    assert not graph.has(AssetKey(["intermediary", "bag_adjacency"]))
