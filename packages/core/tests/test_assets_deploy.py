from bag3d.core.assets.deploy.servers import (
    compressed_export_nl,
    transfer_to_godzilla,
    transfer_to_podzilla,
)
from pathlib import Path
import pytest


@pytest.mark.needs_tools
def test_transfer_to_podzilla(context, test_data_dir):
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
        res = transfer_to_podzilla(
            context,
            compressed_file,
            metadata_file,
        )
        assert (
            res == f"{context.resources.podzilla_server.target_dir}/test_version"
        )  # Check that the function returns a value
    finally:
        # Clean up the test files
        compressed_file.unlink(missing_ok=True)
        metadata_file.unlink(missing_ok=True)
        empty_file.unlink(missing_ok=True)
        export_dir.rmdir()
        with context.resources.podzilla_server.connect as c:
            c.run(
                f"rm -rf {context.resources.podzilla_server.target_dir}/test_version",
                warn=True,
            )
            c.run(
                f"rm -f {context.resources.podzilla_server.target_dir}/export_test_version.tar.gz",
                warn=True,
            )


@pytest.mark.needs_tools
def test_transfer_to_godzilla(context, test_data_dir):
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
        res = transfer_to_godzilla(
            context,
            compressed_file,
            metadata_file,
        )
        assert (
            res == f"{context.resources.godzilla_server.target_dir}/test_version"
        )  # Check that the function returns a value
    finally:
        # Clean up the test files
        compressed_file.unlink(missing_ok=True)
        metadata_file.unlink(missing_ok=True)
        empty_file.unlink(missing_ok=True)
        export_dir.rmdir()
        with context.resources.godzilla_server.connect as c:
            c.run(
                f"rm -rf {context.resources.godzilla_server.target_dir}/test_version",
                warn=True,
            )
            c.run(
                f"rm -f {context.resources.godzilla_server.target_dir}/export_test_version.tar.gz",
                warn=True,
            )
