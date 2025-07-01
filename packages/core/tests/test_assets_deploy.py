from bag3d.core.assets.deploy.godzilla import (
    compressed_export_nl,
    downloadable_godzilla,
)
from pathlib import Path
from fabric import Connection
import pytest


@pytest.mark.needs_tools
def test_downloadable_godzilla(context, test_data_dir):
    # Create deployment dir
    export_dir = test_data_dir / "deployment" / "3DBAG" / "export_test_version"
    export_dir.mkdir(parents=True, exist_ok=True)

    # Create an empty file within the directory
    empty_file = export_dir / "dummy.txt"
    empty_file.touch()

    # Create a mock metadata file
    metadata_file = test_data_dir / "deployment" / "3DBAG" / "metadata.json"
    metadata_file.touch()
    metadata_file.write_text(
        '{"identificationInfo": {"citation": {"edition": "test_version"}}}'
    )
    try:
        # compress the export dir
        res = compressed_export_nl(context, export_dir)

        compressed_file = Path(res.metadata["path"].text)
        assert compressed_file.exists()  # Check that the file was created

        # Test the transfer to godzilla
        res = downloadable_godzilla(
            context,
            compressed_file,
            metadata_file,
            data_dir="/tmp",
            public_dir="/tmp/gina_public",
        )
        assert res == "/tmp/test_version"  # Check that the function returns a value
    finally:
        # Clean up the test files
        compressed_file.unlink(missing_ok=True)
        metadata_file.unlink(missing_ok=True)
        empty_file.unlink(missing_ok=True)
        export_dir.rmdir()
        with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
            c.run("rm -rf /tmp/test_version", warn=True)
            c.run("rm -rf /tmp/gina_public", warn=True)
            c.run("rm -f /tmp/export_test_version.tar.gz", warn=True)
