from dagster import load_assets_from_package_module, load_assets_from_modules

from bag3d.core.assets import (
    ahn,
    bag,
    bgt,
    cbs,
    top10nl,
    input,
    reconstruction,
)

BAG = "bag"
BGT = "bgt"
CBS = "cbs"
TOP10NL = "top10nl"
AHN = "ahn"
INPUT = "input"
RECONSTRUCTION = "reconstruction"

ahn_assets = load_assets_from_package_module(
    package_module=ahn, key_prefix="ahn", group_name=AHN
)

bag_assets = load_assets_from_package_module(
    package_module=bag, key_prefix="bag", group_name=BAG
)

bgt_assets = load_assets_from_package_module(
    package_module=bgt, key_prefix="bgt", group_name=BGT
)

cbs_assets = load_assets_from_package_module(
    package_module=cbs, key_prefix="cbs", group_name=CBS
)

top10nl_assets = load_assets_from_package_module(
    package_module=top10nl, key_prefix="top10nl", group_name=TOP10NL
)

source_assets = [*ahn_assets, *bag_assets, *cbs_assets, *top10nl_assets]

input_assets = load_assets_from_package_module(
    package_module=input, key_prefix="input", group_name=INPUT
)

reconstruction_assets = load_assets_from_package_module(
    package_module=reconstruction,
    key_prefix="reconstruction",
    group_name=RECONSTRUCTION,
)

from bag3d.core.assets import integration_data  # noqa: E402

integration_data_assets = load_assets_from_modules(
    [integration_data], group_name="integration_data"
)

all_assets = [
    *bgt_assets,
    *source_assets,
    *input_assets,
    *reconstruction_assets,
    *integration_data_assets,
]
