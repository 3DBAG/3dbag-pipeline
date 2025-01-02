from dagster import load_assets_from_package_module

from bag3d.core.assets import ahn, bag, top10nl, input, reconstruction, export, deploy

BAG = "bag"
TOP10NL = "top10nl"
AHN = "ahn"
INPUT = "input"
RECONSTRUCTION = "reconstruction"
EXPORT = "export"
DEPLOY = "deploy"

ahn_assets = load_assets_from_package_module(
    package_module=ahn, key_prefix="ahn", group_name=AHN
)

bag_assets = load_assets_from_package_module(
    package_module=bag, key_prefix="bag", group_name=BAG
)

top10nl_assets = load_assets_from_package_module(
    package_module=top10nl, key_prefix="top10nl", group_name=TOP10NL
)

source_assets = ahn_assets + bag_assets + top10nl_assets

input_assets = load_assets_from_package_module(
    package_module=input, key_prefix="input", group_name=INPUT
)

reconstruction_assets = load_assets_from_package_module(
    package_module=reconstruction,
    key_prefix="reconstruction",
    group_name=RECONSTRUCTION,
)

export_assets = load_assets_from_package_module(
    package_module=export, key_prefix="export", group_name=EXPORT
)

deploy_assets = load_assets_from_package_module(
    package_module=deploy, key_prefix="deploy", group_name=DEPLOY
)
