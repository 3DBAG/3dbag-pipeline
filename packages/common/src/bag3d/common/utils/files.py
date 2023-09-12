from pathlib import Path
from zipfile import ZipFile

from dagster import get_dagster_logger


class BadArchiveError(OSError):
    pass


def unzip(file: Path, dest: Path):
    """Uncompress the whole zip archive and delete the zip.

    Args:
        file: The Path to the zip.
        dest: The Path to the destination directory.

    Returns:
        None
    """
    logger = get_dagster_logger()
    logger.info(f"Uncompressing {file} to {dest}")
    with ZipFile(file, 'r') as ezip:
        first_bad_file = ezip.testzip()
        if first_bad_file:
            raise BadArchiveError(
                f"The archive contains at least one bad file: {first_bad_file}")
        ezip.extractall(path=dest)
    logger.info(f"Deleting {file}")
    file.unlink()


def bag3d_dir(root_dir: Path):
    return Path(root_dir) / "3DBAG"


def geoflow_crop_dir(root_dir):
    return bag3d_dir(root_dir) / "crop_reconstruct"


def bag3d_export_dir(root_dir):
    """Create the 3DBAG export directory"""
    export_dir = bag3d_dir(root_dir) / "export"
    export_dir.mkdir(exist_ok=True)
    return export_dir
