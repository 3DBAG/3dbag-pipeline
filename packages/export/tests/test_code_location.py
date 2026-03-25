import subprocess
import sys


def test_code_location_import_fresh_process():
    """Ensure bag3d.export.code_location can be imported without DagsterInvalidDefinitionError.

    Uses a subprocess to avoid conftest.py side-effects that pre-register Dagster types,
    masking import-order conflicts.
    """
    result = subprocess.run(
        [sys.executable, "-c", "import bag3d.export.code_location"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Importing bag3d.export.code_location failed:\n{result.stderr}"
    )
