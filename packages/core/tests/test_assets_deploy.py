from bag3d.core.assets.deploy.servers import compressed_export_nl, transfer_to_server
from pathlib import Path
import pytest


@pytest.mark.skip("included in integration test")
def test_transfer_to_server(context, deployment_server, test_data_dir):
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

        # Test the transfer to podzilla
        res = transfer_to_server(
            server=deployment_server,
            compressed_export_nl=compressed_file,
            metadata=metadata_file,
            target_dir=deployment_server.target_dir,
        )
        assert (
            res == f"{deployment_server.target_dir}/test_version"
        )  # Check that the function returns a value
        assert deployment_server.file_exists(
            f"{deployment_server.target_dir}/test_version/dummy.txt"
        )
    finally:
        # Clean up the test files
        compressed_file.unlink(missing_ok=True)
        metadata_file.unlink(missing_ok=True)
        empty_file.unlink(missing_ok=True)
        export_dir.rmdir()
