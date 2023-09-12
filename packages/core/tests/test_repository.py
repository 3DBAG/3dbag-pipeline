from dagster import load_assets_from_package_module

from bag3d.core.assets import ahn


def test_load_ahn_assets():
    ahn_assets = load_assets_from_package_module(
        package_module=ahn,
        key_prefix="ahn",
        group_name="source"
    )
    [print(a.keys) for a in ahn_assets]
