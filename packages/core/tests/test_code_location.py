import importlib
import subprocess
import sys

from bag3d.core.assets import ahn
from dagster import load_assets_from_package_module, Definitions, AssetsDefinition


def test_definitions_loadable():
    # Import your definitions module
    module = importlib.import_module("bag3d.core.code_location")
    defs = getattr(module, "defs")

    # This will raise an error if definitions are not loadable
    Definitions.validate_loadable(defs)


def test_code_location_import_fresh_process():
    """Regression test: ensure code_location can be imported without DagsterInvalidDefinitionError.

    Uses a subprocess to avoid the conftest.py side-effect where
    'from bag3d.common.types import PostgresTableIdentifier' pre-registers the type,
    masking import-order conflicts.

    Root cause: ahn/metadata.py must import PostgresTableIdentifier from
    bag3d.common.types (not pgutils) so explicit registration happens before
    Dagster processes asset return-type annotations.
    """
    result = subprocess.run(
        [sys.executable, "-c", "import bag3d.core.code_location"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Importing bag3d.core.code_location failed:\n{result.stderr}"
    )


def test_load_ahn_assets():
    ahn_assets = load_assets_from_package_module(
        package_module=ahn, key_prefix="ahn", group_name="source"
    )
    for a in ahn_assets:
        if isinstance(a, AssetsDefinition):
            print(a.keys)
