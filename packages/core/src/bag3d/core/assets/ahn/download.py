from pathlib import Path
from typing import Tuple, Mapping, Union, Any
from hashlib import new as hash_new, algorithms_available
from dataclasses import dataclass

from dagster import asset, Output, get_dagster_logger

from bag3d.common.utils.requests import download_file, download_as_str
from bag3d.core.assets.ahn.core import (
    PartitionDefinitionAHN,
    format_laz_log,
    ahn_filename,
    download_ahn_index,
    ahn_laz_dir,
)

logger = get_dagster_logger("ahn.download")

# AHN LAZ file MD5 sums computed at PDOK
# TODO: Add checksum for AHN5
URL_LAZ_SHA = {
    "ahn4": "https://gist.githubusercontent.com/fwrite/6bb4ad23335c861f9f3162484e57a112/raw/ee5274c7c6cf42144d569e303cf93bcede3e2da1/AHN4.md5",
    "ahn3": "https://gist.githubusercontent.com/arbakker/dcca00384cddbdf10c0421ed26d8911c/raw/f43465d287a654254e21851cce38324eba75d03c/checksum_laz.md5",
}


class HashChunkwise:
    """Compute the MD5/SHA256 of the contents of a file, reading by chunks.

    Read chunks of 4096 bytes sequentially and feed them to md5, because
    the file is too big to fit into the memory.

    Args:
        method (str): One of the hashing algorithms available in ``hashlib``.
    """

    def __init__(self, method: str):
        self.method = method

    @property
    def method(self):
        return self._method

    @method.setter
    def method(self, value):
        if value in algorithms_available:
            self._method = value
        else:  # pragma: no cover
            raise ValueError(
                f"The hashing algorithm {value} is not available in " f"hashlib."
            )

    def compute(self, fpath: Path):
        """Compute the hash of a file.

        Read chunks of 4096 bytes sequentially and feed them to the hashing function,
        because the file is too big to fit into the memory.

        Returns:
            A hashlib.HASH object.
        """
        return self._compute_hash_chunkwise(fpath)

    def _compute_hash_chunkwise(self, fpath: Path):
        hash_ = hash_new(self.method)
        with fpath.open("rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_.update(chunk)
        return hash_


@dataclass
class LAZDownload:
    """AHN LAZ download result.

    Args:
        url(str): Url where the Laz file was downloaded from
        path (Path): The Path to the LAZ file.
        success (bool): Operations succeeded on the file.
        hash_name (Optional[hashlib.HASH]): The hash object returned from the hashing
            function.
        new (bool): The file is newly downloaded.
        size (float): File size in Mb.
    """

    url: str
    path: Path
    success: bool
    hash_name: Union[str, None]
    hash_hexdigest: Union[str, None]
    new: bool
    size: float

    def asdict(self) -> dict:
        return {
            "Url": self.url,
            "Path": str(self.path),
            "Success": self.success,
            "Hash": f"{self.hash_name}:{self.hash_hexdigest}",
            "New": self.new,
            "Size [Mb]": self.size,
        }

    def validate(
        self, sha_reference: Mapping[str, str], sha_func: HashChunkwise
    ) -> bool:
        """Compare the SHA of the local file to the provided reference."""
        match, sha = match_sha(
            fpath=self.path, sha_reference=sha_reference, sha_func=sha_func
        )

        self.hash_name = sha.name
        self.hash_hexdigest = sha.hexdigest()
        if match:
            logger.debug(format_laz_log(self.path, "OK"))
        return match


@asset
def md5_pdok_ahn3(context):
    """Download the MD5 sums that are calculated by PDOK for the AHN3 LAZ files."""
    return get_md5_pdok(URL_LAZ_SHA["ahn3"])


@asset
def md5_pdok_ahn4(context):
    """Download the MD5 sums that are calculated by PDOK for the AHN4 LAZ files."""
    return get_md5_pdok(URL_LAZ_SHA["ahn4"])


@asset
def tile_index_ahn(context):
    """The AHN tile index, including the tile geometry and the file download links."""
    return download_ahn_index(with_geom=True)


@asset(
    required_resource_keys={"file_store"},
    partitions_def=PartitionDefinitionAHN(),
)
def laz_files_ahn3(context, md5_pdok_ahn3, tile_index_ahn):
    """AHN3 LAZ files as they are downloaded from PDOK.

    The download links are retrieved from the AHN tile index service (blaadindex).
    Only download a file if it does not exist locally, or the SHA of the file does not
    match the reference.
    """
    tile_id = context.partition_key
    laz_dir = ahn_laz_dir(context.resources.file_store.file_store.data_dir, 3)
    laz_dir.mkdir(exist_ok=True, parents=True)
    fpath = laz_dir / ahn_filename(tile_id)
    url_laz = tile_index_ahn[tile_id]["AHN3_LAZ"]
    # Because https://ns_hwh.fundaments.nl is not configured properly.
    # Check with https://www.digicert.com/help/
    verify_ssl = False
    lazdownload = download_ahn_laz(
        fpath=fpath,
        url_laz=url_laz,
        verify_ssl=verify_ssl,
    )
    first_validation = lazdownload.validate(
        sha_reference=md5_pdok_ahn3, sha_func=HashChunkwise("md5")
    )

    # Let's try to re-download the file once
    if not first_validation:
        logger.info(format_laz_log(fpath, "Removing"))
        fpath.unlink()
        lazdownload = download_ahn_laz(
            fpath=fpath, url_laz=url_laz, verify_ssl=verify_ssl
        )
        second_validation = lazdownload.validate(
            sha_reference=md5_pdok_ahn3, sha_func=HashChunkwise("md5")
        )
        if not second_validation:
            logger.error(format_laz_log(fpath, "ERROR"))
            lazdownload = LAZDownload(
                url=None,
                path=Path(),
                success=False,
                hash_name=None,
                hash_hexdigest=None,
                new=False,
                size=0.0,
            )
    else:
        logger.debug(format_laz_log(fpath, "OK"))

    return Output(lazdownload, metadata=lazdownload.asdict())


@asset(
    required_resource_keys={"file_store"},
    partitions_def=PartitionDefinitionAHN(),
)
def laz_files_ahn4(context, md5_pdok_ahn4, tile_index_ahn):
    """AHN4 LAZ files as they are downloaded from PDOK.

    The download links are retrieved from the AHN tile index service (blaadindex).
    Only downloads a file if it does not exist locally, or the SHA of the file does not
    match the reference.
    """
    tile_id = context.partition_key

    laz_dir = ahn_laz_dir(context.resources.file_store.file_store.data_dir, 4)
    laz_dir.mkdir(exist_ok=True, parents=True)
    fpath = laz_dir / ahn_filename(tile_id)
    url_laz = tile_index_ahn[tile_id]["AHN4_LAZ"]
    # Because https://ns_hwh.fundaments.nl is not configured properly.
    # Check with https://www.digicert.com/help/
    verify_ssl = False
    lazdownload = download_ahn_laz(
        fpath=fpath,
        url_laz=url_laz,
        verify_ssl=verify_ssl,
    )
    first_validation = lazdownload.validate(
        sha_reference=md5_pdok_ahn4, sha_func=HashChunkwise("md5")
    )

    # Let's try to re-download the file once
    if not first_validation:
        logger.info(format_laz_log(fpath, "Removing"))
        fpath.unlink()
        lazdownload = download_ahn_laz(
            fpath=fpath,
            url_laz=url_laz,
            verify_ssl=verify_ssl,
        )
        second_validation = lazdownload.validate(
            sha_reference=md5_pdok_ahn4, sha_func=HashChunkwise("md5")
        )
        if not second_validation:
            logger.error(format_laz_log(fpath, "ERROR"))
            lazdownload = LAZDownload(
                url=None,
                path=Path(),
                success=False,
                hash_name=None,
                hash_hexdigest=None,
                new=False,
                size=0.0,
            )
    else:
        logger.debug(format_laz_log(fpath, "OK"))

    return Output(lazdownload, metadata=lazdownload.asdict())


@asset(
    required_resource_keys={"file_store"},
    partitions_def=PartitionDefinitionAHN(),
)
def laz_files_ahn5(context, tile_index_ahn):
    """AHN5 LAZ files as they are downloaded from PDOK.

    The download links are retrieved from the AHN tile index service (blaadindex).
     Only downloads a file if it does not exist locally.
     At the moment there is no sha validation for AHN5.
    """
    tile_id = context.partition_key
    laz_dir = ahn_laz_dir(context.resources.file_store.file_store.data_dir, 5)
    laz_dir.mkdir(exist_ok=True, parents=True)
    fpath = laz_dir / ahn_filename(tile_id)
    url_laz = tile_index_ahn[tile_id]["AHN5_LAZ"]
    # Because https://ns_hwh.fundaments.nl is not configured properly.
    # Check with https://www.digicert.com/help/
    verify_ssl = False
    lazdownload = download_ahn_laz(
        fpath=fpath,
        url_laz=url_laz,
        verify_ssl=verify_ssl,
    )
    # TODO: Add validation when checksum become available.
    return Output(lazdownload, metadata=lazdownload.asdict())


# @multi_asset(
#     required_resource_keys={"file_store", "pdal"},
#     partitions_def=PartitionDefinitionAHN(),
#     ins={
#         "md5_pdok_ahn3": AssetIn(["ahn", "md5_pdok_ahn3"])
#     },
#     outs={
#         "laz_files_ahn3": Out(is_required=False),
#         "pdal_info_ahn3": Out(is_required=False)
#     },
#     can_subset=True
# )
# def multi_laz_files_ahn3(context, md5_pdok_ahn3):
#     """AHN3 LAZ files as they are downloaded from PDOK.
#
#     Only downlaod a file if it does not exist locally, or the SHA of the file does not
#     match the reference.
#     """
#     tile_id = context.partition_key
#
#     fpath = context.resources.file_store.file_store.data_dir / ahn_filename(tile_id)
#     fpath = Path(f"/data/AHN3/tiles_200m/t_{tile_id}.laz")
#     if "laz_files" in context.selected_output_names:
#         fpath = context.resources.file_store.file_store.data_dir / ahn_filename(tile_id)
#         lazdownload = download_ahn_laz(fpath=fpath, url_base=URL_LAZ["ahn3"],
#                                        sha_reference=md5_pdok_ahn3,
#                                        sha_func=HashChunkwise("md5"))
#         fpath = lazdownload.path
#         yield Output(lazdownload.path, output_name="laz_files_ahn3",
#                      metadata=lazdownload.asdict())
#     if "pdal_info" in context.selected_output_names:
#         ret_code, out_info = pdal_info(context.resources.pdal, file_path=fpath,
#                                        with_all=False)
#         yield Output(fpath, metadata={**out_info}, output_name="pdal_info_ahn3")


def get_md5_pdok(url: str) -> Mapping[str, str]:
    """Download the MD5 sum of AHN3 LAZ files from PDOK.

    Returns:
         { filename: md5 }
    """
    _md5 = download_as_str(url)
    md5_pdok = {}
    for tile in _md5.strip().split("\n"):
        sha, file = tile.split()
        md5_pdok[file] = sha
    return md5_pdok


def download_ahn_laz(
    fpath: Path, url_laz: str = None, url_base: str = None, verify_ssl: bool = False
) -> LAZDownload:
    """Download an AHN LAZ file from the input url to the given path,
    if the file does not exists.

    Args:
        fpath: Path to the LAZ file that may exist locally. If not it will be downloaded.
        url_laz: Complete URL of the file to download. If provided, 'url_base' is
            ignored.
        url_base (str): Base URL for the file to be downloaded.
        verify_ssl (bool): Whether to verify the SSL certificate of the URL.

    Returns:
        A LAZDownload file
    """

    url = url_laz if url_laz is not None else "/".join([url_base, fpath.name])

    if not fpath.is_file():
        logger.info(format_laz_log(fpath, "Not found. Downloading..."))
        fpath = download_file(
            url=url, target_path=fpath.parent, chunk_size=1024 * 1024, verify=verify_ssl
        )
        if fpath is None:
            # Download failed
            logger.warning(format_laz_log(fpath, "Downloading failed!"))
            return LAZDownload(
                url=None,
                path=Path(),
                success=False,
                hash_name=None,
                hash_hexdigest=None,
                new=False,
                size=0.0,
            )
        else:
            is_new = True
    else:  # pragma: no cover
        is_new = False
    return LAZDownload(
        url=url_laz,
        path=fpath,
        success=True,
        hash_name=None,
        hash_hexdigest=None,
        new=is_new,
        size=round(fpath.stat().st_size / 1e6, 2),
    )


def match_sha(
    fpath: Path, sha_reference: Mapping[str, str], sha_func: HashChunkwise
) -> Tuple[bool, Any]:
    """Verify the SHA of a file against a reference.

    Args:
        fpath: Path to the file
        sha_reference: Reference SHA sums to match against,
            as { filename : SHA }
        sha_func: An SHA function object

    Returns:
        Tuple of (success, SHA).
    """
    if not fpath.is_file():  # pragma: no cover
        raise FileNotFoundError(fpath)
    sha = sha_func.compute(fpath)
    if sha.hexdigest() == sha_reference[fpath.name]:
        logger.info(format_laz_log(fpath, f"{sha.name} OK"))
        return True, sha
    else:  # pragma: no cover
        logger.info(format_laz_log(fpath, f"{sha.name} mismatch"))
        return False, sha
