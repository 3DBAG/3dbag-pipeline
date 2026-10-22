import os
import subprocess
import sys
from pathlib import Path

import pytest
from dagster import AssetKey, Definitions

from bag3d.terrain.code_location import defs
from bag3d.terrain.jobs import DOWNLOAD_ASSETS
from bag3d.terrain.settings import TerrainSettings


def test_definitions_and_job_selections():
    Definitions.validate_loadable(defs)
    graph = defs.resolve_asset_graph()
    downloads = {AssetKey(["terrain", name]) for name in DOWNLOAD_ASSETS}
    download_job = defs.resolve_job_def("terrain_download")
    process_job = defs.resolve_job_def("terrain_process")
    assert download_job.asset_layer.executable_asset_keys == downloads
    assert not downloads & set(process_job.asset_layer.executable_asset_keys)
    assert (
        len(set(defs.resolve_job_def("terrain").asset_layer.executable_asset_keys))
        == 13
    )
    assert graph.get(AssetKey(["terrain", "ahn_ellipsoid"])).parent_keys == {
        AssetKey(["terrain", "ahn_filled"]),
        AssetKey(["terrain", "proj_grids_download"]),
    }
    assert defs.resources is not None
    assert set(defs.resources) == {"file_store", "terrain"}


def test_import_has_no_network_database_or_filesystem_work(tmp_path):
    script = """
import socket
socket.socket.connect = lambda *a, **kw: (_ for _ in ()).throw(AssertionError('network during import'))
from bag3d.terrain.code_location import defs
from dagster import Definitions
Definitions.validate_loadable(defs)
"""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("BAG3D_PG_")
    }
    env["DAGSTER_DEPLOYMENT"] = "production"
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "settings",
    [
        {"bounds": [5, 52, 4, 53]},
        {"bounds": [4, 52, 5]},
        {"bounds": [4, 52, 5, float("nan")]},
        {"base_url": "/terrain"},
        {"interpolation_radius_m": float("inf")},
        {"workers": 0},
    ],
)
def test_invalid_settings(settings):
    with pytest.raises(ValueError):
        TerrainSettings(**settings)


def test_workers_and_url_do_not_change_processing_identity():
    assert (
        TerrainSettings(workers=1).processing_signature()
        == TerrainSettings(
            workers=8, base_url="https://example.com/terrain"
        ).processing_signature()
    )


def test_python_file_workspace_entrypoint():
    from dagster._core.code_pointer import load_python_file

    from bag3d.terrain import code_location

    module = load_python_file(
        str(Path(code_location.__file__).resolve()), str(Path.cwd())
    )
    Definitions.validate_loadable(module.defs)
