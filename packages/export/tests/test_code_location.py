import os
import subprocess
import sys

import pytest
from bag3d.common.resources.files import TilesDirResource
from pydantic import ValidationError


@pytest.mark.parametrize("name", ["", ".", "..", "tile/subdir", "/tiles", "a b"])
def test_tiles_dir_rejects_names_tyler_cannot_use(name):
    with pytest.raises(ValidationError, match="Use a tiles directory name"):
        TilesDirResource(name=name)


def test_code_location_import_fresh_process():
    """Ensure the export job resolves without DagsterInvalidDefinitionError.

    Uses a subprocess to avoid conftest.py side-effects that pre-register Dagster types,
    masking import-order conflicts.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from dagster import AssetKey, RunConfig, asset, materialize
from bag3d.common.resources import resource_defs
from bag3d.export.code_location import defs
from bag3d.common.resources.files import TilesDirResource

graph = defs.resolve_asset_graph()
quadtree_key = AssetKey(('export', 'merged_quadtree'))
assert graph.get(AssetKey(('export', 'geopackage'))).parent_keys == {quadtree_key}
assert graph.get(AssetKey(('export', 'export_index'))).parent_keys == {quadtree_key}
tilesets = {AssetKey(('export', 'reconstruction_output_3dtiles_' + lod)) for lod in ('lod12', 'lod13', 'lod22')}
assert tilesets <= graph.get(AssetKey(('export', 'compressed_tiles_validation'))).parent_keys
defs.get_job_def('export')

@asset
def directory_name(tiles_dir: TilesDirResource) -> str:
    return tiles_dir.name

assert defs.resources['tiles_dir'] is resource_defs['tiles_dir']
resources = {'tiles_dir': resource_defs['tiles_dir']}
result = materialize([directory_name], resources=resources)
assert result.output_for_node('directory_name') == 'tiles'
result = materialize(
    [directory_name],
    resources=resources,
    run_config=RunConfig(resources={'tiles_dir': TilesDirResource(name='t')}),
)
assert result.output_for_node('directory_name') == 't'
""",
        ],
        env={**os.environ, "DAGSTER_DEPLOYMENT": "default"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Importing bag3d.export.code_location failed:\n{result.stderr}"
    )
