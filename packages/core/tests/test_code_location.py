import importlib
from bag3d.core.assets import ahn
from dagster import load_assets_from_package_module, Definitions, AssetsDefinition


def test_definitions_loadable():
    # Import your definitions module
    module = importlib.import_module("bag3d.core.code_location")
    defs = getattr(module, "defs")

    # This will raise an error if definitions are not loadable
    Definitions.validate_loadable(defs)


def test_load_ahn_assets():
    ahn_assets = load_assets_from_package_module(
        package_module=ahn, key_prefix="ahn", group_name="source"
    )
    for a in ahn_assets:
        if isinstance(a, AssetsDefinition):
            print(a.keys)
