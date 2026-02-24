from dagster import load_assets_from_package_module

from bag3d.core.assets import (
    ahn,
    bag,
    bgt,
    top10nl,
    input,
    reconstruction,
    export,
    deploy,
    release,
)

BAG = "bag"
BGT = "bgt"
TOP10NL = "top10nl"
AHN = "ahn"
INPUT = "input"
RECONSTRUCTION = "reconstruction"
EXPORT = "export"
DEPLOY = "deploy"
RELEASE = "release"

ahn_assets = load_assets_from_package_module(
    package_module=ahn, key_prefix="ahn", group_name=AHN
)

bag_assets = load_assets_from_package_module(
    package_module=bag, key_prefix="bag", group_name=BAG
)

bgt_assets = load_assets_from_package_module(
    package_module=bgt, key_prefix="bgt", group_name=BGT
)

top10nl_assets = load_assets_from_package_module(
    package_module=top10nl, key_prefix="top10nl", group_name=TOP10NL
)

source_assets = [*ahn_assets, *bag_assets, *top10nl_assets]

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

release_assets = load_assets_from_package_module(
    package_module=release, key_prefix="release", group_name=RELEASE
)
