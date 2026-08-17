import importlib
import subprocess
import sys

from dagster import Definitions


def test_definitions_loadable():
    # Import your definitions module
    module = importlib.import_module("bag3d.core.code_location")
    defs = module.defs

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
        check=False,
    )
    assert result.returncode == 0, (
        f"Importing bag3d.core.code_location failed:\n{result.stderr}"
    )
