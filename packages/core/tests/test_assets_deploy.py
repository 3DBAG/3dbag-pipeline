from bag3d.common.resources import ServerTransferResource
from bag3d.core.assets.deploy.servers import compressed_export_nl, transfer_to_server
from pathlib import Path
import pytest


def test_transfer_to_server(context, test_data_dir):
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
        target_dir = "/data/3DBAG"
        server = ServerTransferResource(
            host="3dbag.docker.internal",
            port=2222,
            user="deploy",
            password="deploy",
            target_dir=target_dir,
            public_dir="/data/3DBAG/public",
        )
        res = transfer_to_server(
            server=server,
            compressed_export_nl=compressed_file,
            metadata=metadata_file,
            target_dir=target_dir,
        )
        assert (
            res == f"{target_dir}/test_version"
        )  # Check that the function returns a value
    finally:
        # Clean up the test files
        compressed_file.unlink(missing_ok=True)
        metadata_file.unlink(missing_ok=True)
        empty_file.unlink(missing_ok=True)
        export_dir.rmdir()
