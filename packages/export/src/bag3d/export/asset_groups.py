from dagster import load_assets_from_package_module

from bag3d.export.assets import (
    deploy,
    export,
    release,
)

EXPORT = "export"
DEPLOY = "deploy"
RELEASE = "release"

export_assets = load_assets_from_package_module(
    package_module=export, key_prefix="export", group_name=EXPORT
)

deploy_assets = load_assets_from_package_module(
    package_module=deploy, key_prefix="deploy", group_name=DEPLOY
)

release_assets = load_assets_from_package_module(
    package_module=release, key_prefix="release", group_name=RELEASE
)

all_assets = [
    *export_assets,
    *deploy_assets,
    *release_assets,
]
