from bag3d.core.assets.deploy.godzilla import compressed_export_nl
from pathlib import Path
import pytest


@pytest.mark.slow
def test_compressed_export_nl(context, test_data_dir):
    export_dir = (
        test_data_dir / "reconstruction_input" / "3DBAG" / "export_test_version"
    )
    res = compressed_export_nl(context, export_dir)

    path = Path(res.metadata["path"].text)
    assert path.exists()  # Check that the file was created

    # Remove the file after the test
    path.unlink()
