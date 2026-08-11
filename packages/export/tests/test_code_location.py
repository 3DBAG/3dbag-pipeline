import subprocess
import sys


def test_code_location_import_fresh_process():
    """Ensure the export job resolves without DagsterInvalidDefinitionError.

    Uses a subprocess to avoid conftest.py side-effects that pre-register Dagster types,
    masking import-order conflicts.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from bag3d.export.code_location import defs; defs.get_job_def('export')",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Importing bag3d.export.code_location failed:\n{result.stderr}"
    )
