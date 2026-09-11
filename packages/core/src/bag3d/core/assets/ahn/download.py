import json
import random
import time
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import algorithms_available
from hashlib import new as hash_new
from pathlib import Path
from typing import Any

import requests
import urllib3
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.utils.requests import download_as_str, download_file
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Config,
    Failure,
    Output,
    asset,
    get_dagster_logger,
)

from bag3d.core.assets.ahn.core import (
    download_ahn6_index,
    download_ahn_index,
    format_laz_log,
    partition_definition_ahn,
    partition_definition_ahn6_batches,
    tiles_in_batch,
)

logger = get_dagster_logger("ahn.download")

# AHN LAZ file with checksums.
URL_LAZ_SHA = {
    6: "https://basisdata.nl/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN6_KM_PC_COPC.json",
    5: "https://fsn1.your-objectstorage.com/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN5_PC.json",
    4: "https://fsn1.your-objectstorage.com/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN4_PC.json",
    3: "https://fsn1.your-objectstorage.com/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN3_PC.json",
}


class HashChunkwise:
    """Compute the checksum of a file's contents, reading it in chunks.

    Chunks of 4096 bytes are read sequentially and fed to the hashing function
    selected by ``method`` (e.g. ``"md5"`` or ``"sha256"``), so the file does
    not need to fit into memory.

    Args:
        method (str): One of the hashing algorithms available in ``hashlib``
            (see :func:`hashlib.algorithms_available`).
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
                f"The hashing algorithm {value} is not available in hashlib."
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
    hash_name: str | None
    hash_hexdigest: str | None
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

    def compute_sha(self, sha_func: HashChunkwise):
        """Compute and store the SHA of the local file."""
        if not self.path.is_file():  # pragma: no cover
            raise FileNotFoundError(self.path)
        sha = sha_func.compute(self.path)
        self.hash_name = sha.name
        self.hash_hexdigest = sha.hexdigest()

    def validate(
        self, sha_reference: Mapping[str, str], sha_func: HashChunkwise
    ) -> bool:
        """Compare the SHA of the local file to the provided reference."""
        self.compute_sha(sha_func=sha_func)
        assert self.hash_name is not None
        assert self.hash_hexdigest is not None
        match = match_sha(
            fpath=self.path,
            sha_reference=sha_reference,
            hash_name=self.hash_name,
            hash_hexdigest=self.hash_hexdigest,
        )
        if match:
            logger.debug(format_laz_log(self.path, "OK"))
        return match


@dataclass
class BatchLAZDownload:
    """Result of a batched AHN6 COPC download.

    Args:
        batch_id: The 10×10 km batch partition key (e.g. ``"150000_460000"``).
        tiles: Dictionary of individual tile download results {tile_id: LAZDownload}.
    """

    batch_id: str
    tiles: dict[str, LAZDownload]

    def asdict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "tiles": {tile_id: t.asdict() for tile_id, t in self.tiles.items()},
        }


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def sha256_ahn3() -> dict[str, str]:
    """Download the SHA256 sums that are calculated by PDOK for the AHN3 LAZ files."""
    return get_checksums(URL_LAZ_SHA, ahn_version=3)


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def sha256_ahn4() -> dict[str, str]:
    """Download the SHA256 sums that are calculated by PDOK for the AHN4 LAZ files."""
    return get_checksums(URL_LAZ_SHA, ahn_version=4)


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def sha256_ahn5() -> dict[str, str]:
    """Download the SHA256 sums for the AHN5 LAZ files, provided by AHN."""
    return get_checksums(URL_LAZ_SHA, ahn_version=5)


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def sha256_ahn6() -> dict[str, str]:
    """Download the SHA256 sums and URLs for the AHN6 COPC.LAZ files, provided by AHN."""
    return get_checksums(URL_LAZ_SHA, ahn_version=6)


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def tile_index_ahn() -> dict[str, dict[str, Any] | None] | None:
    """The AHN tile index, including the tile geometry and the file download links."""
    return download_ahn_index(with_geom=True)


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def tile_index_ahn6() -> dict[str, dict[str, Any] | None] | None:
    """The AHN6 tile index, including the tile geometry and the file download links."""
    return download_ahn6_index(with_geom=True)


class LazFilesConfig(Config):
    force_download: bool = False
    check_hash: bool = True


@asset(
    partitions_def=partition_definition_ahn,
    pool="laz_download",
)
def laz_files_ahn3(
    context: AssetExecutionContext,
    config: LazFilesConfig,
    pointcloud_store: FileStoreResource,
    sha256_ahn3: dict[str, str],
    tile_index_ahn,
) -> Output[LAZDownload]:
    """AHN3 LAZ files as they are downloaded from PDOK.

    The download links are retrieved from the AHN tile index service (blaadindex).
    Only download a file if it does not exist locally, or the SHA of the file does not
    match the reference.
    """
    tile_id = context.partition_key
    laz_dir = pointcloud_store.create_subdir("AHN3/as_downloaded/LAZ")
    url_laz = tile_index_ahn[tile_id]["AHN3_LAZ"]
    fpath = laz_dir / url_laz.split("/")[-1]
    # Because https://ns_hwh.fundaments.nl is not configured properly.
    # Check with https://www.digicert.com/help/
    verify_ssl = False
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=urllib3.exceptions.InsecureRequestWarning
        )
        lazdownload = download_ahn_laz(
            fpath=fpath,
            url_laz=url_laz,
            verify_ssl=verify_ssl,
            force_download=config.force_download,
        )
    lazdownload.compute_sha(HashChunkwise("sha256"))
    if config.check_hash:
        first_validation = lazdownload.validate(
            sha_reference=sha256_ahn3, sha_func=HashChunkwise("sha256")
        )

        # Let's try to re-download the file once
        if not first_validation:
            logger.info(
                format_laz_log(
                    fpath, "First validation failed. Removing and retrying..."
                )
            )
            fpath.unlink()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", category=urllib3.exceptions.InsecureRequestWarning
                )
                lazdownload = download_ahn_laz(
                    fpath=fpath, url_laz=url_laz, verify_ssl=verify_ssl
                )
            second_validation = lazdownload.validate(
                sha_reference=sha256_ahn3, sha_func=HashChunkwise("sha256")
            )
            if not second_validation:
                logger.warning(format_laz_log(fpath, "Checksum failed"))
        else:
            logger.debug(format_laz_log(fpath, "Validation OK"))

    return Output(lazdownload, metadata=lazdownload.asdict())


@asset(
    partitions_def=partition_definition_ahn,
    pool="laz_download",
)
def laz_files_ahn4(
    context: AssetExecutionContext,
    config: LazFilesConfig,
    pointcloud_store: FileStoreResource,
    sha256_ahn4: dict[str, str],
    tile_index_ahn,
) -> Output[LAZDownload]:
    """AHN4 LAZ files as they are downloaded from PDOK.

    The download links are retrieved from the AHN tile index service (blaadindex).
    Only downloads a file if it does not exist locally, or the SHA of the file does not
    match the reference.
    """
    tile_id = context.partition_key

    laz_dir = pointcloud_store.create_subdir("AHN4/as_downloaded/LAZ")
    url_laz = tile_index_ahn[tile_id]["AHN4_LAZ"]
    fpath = laz_dir / url_laz.split("/")[-1]
    # Because https://ns_hwh.fundaments.nl is not configured properly.
    # Check with https://www.digicert.com/help/
    verify_ssl = False
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=urllib3.exceptions.InsecureRequestWarning
        )
        lazdownload = download_ahn_laz(
            fpath=fpath,
            url_laz=url_laz,
            verify_ssl=verify_ssl,
            force_download=config.force_download,
        )
    lazdownload.compute_sha(HashChunkwise("sha256"))
    if config.check_hash:
        first_validation = lazdownload.validate(
            sha_reference=sha256_ahn4, sha_func=HashChunkwise("sha256")
        )

        # Let's try to re-download the file once
        if not first_validation:
            logger.info(
                format_laz_log(
                    fpath, "First validation failed. Removing and retrying..."
                )
            )
            fpath.unlink()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", category=urllib3.exceptions.InsecureRequestWarning
                )
                lazdownload = download_ahn_laz(
                    fpath=fpath,
                    url_laz=url_laz,
                    verify_ssl=verify_ssl,
                )
            second_validation = lazdownload.validate(
                sha_reference=sha256_ahn4, sha_func=HashChunkwise("sha256")
            )
            if not second_validation:
                logger.warning(format_laz_log(fpath, "Checksum failed"))
        else:
            logger.debug(format_laz_log(fpath, "Validation OK"))

    return Output(lazdownload, metadata=lazdownload.asdict())


@asset(
    partitions_def=partition_definition_ahn,
    pool="laz_download",
)
def laz_files_ahn5(
    context: AssetExecutionContext,
    config: LazFilesConfig,
    pointcloud_store: FileStoreResource,
    sha256_ahn5: dict[str, str],
    tile_index_ahn,
) -> Output[LAZDownload]:
    """AHN5 LAZ files as they are downloaded from PDOK.

    The download links are retrieved from the AHN tile index service (blaadindex).
    Only downloads a file if it does not exist locally.
    """
    tile_id = context.partition_key
    laz_dir = pointcloud_store.create_subdir("AHN5/as_downloaded/LAZ")
    url_laz = tile_index_ahn[tile_id]["AHN5_LAZ"]
    fpath = laz_dir / url_laz.split("/")[-1]
    # Because https://ns_hwh.fundaments.nl is not configured properly.
    # Check with https://www.digicert.com/help/
    verify_ssl = False
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=urllib3.exceptions.InsecureRequestWarning
        )
        lazdownload = download_ahn_laz(
            fpath=fpath,
            url_laz=url_laz,
            verify_ssl=verify_ssl,
            force_download=config.force_download,
        )
    lazdownload.compute_sha(HashChunkwise("sha256"))
    if config.check_hash:
        first_validation = lazdownload.validate(
            sha_reference=sha256_ahn5, sha_func=HashChunkwise("sha256")
        )
        # Let's try to re-download the file once
        if not first_validation:
            logger.info(
                format_laz_log(
                    fpath, "First validation failed. Removing and retrying..."
                )
            )
            fpath.unlink()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", category=urllib3.exceptions.InsecureRequestWarning
                )
                lazdownload = download_ahn_laz(
                    fpath=fpath,
                    url_laz=url_laz,
                    verify_ssl=verify_ssl,
                )
            second_validation = lazdownload.validate(
                sha_reference=sha256_ahn5, sha_func=HashChunkwise("sha256")
            )
            if not second_validation:
                logger.warning(format_laz_log(fpath, "Checksum failed"))
        else:
            logger.debug(format_laz_log(fpath, "Validation OK"))

    return Output(lazdownload, metadata=lazdownload.asdict())


@asset(
    name="laz_files_ahn6",
    partitions_def=partition_definition_ahn6_batches,
    pool="laz_download",
)
def laz_files_ahn6(
    context: AssetExecutionContext,
    config: LazFilesConfig,
    pointcloud_store: FileStoreResource,
    sha256_ahn6: dict[str, str],
    tile_index_ahn6: dict[str, dict[str, Any]],
) -> Output[BatchLAZDownload]:
    """Download AHN6 COPC pointclouds on the 1x1 km tile grid.

    Each partition is a 10x10 km block containing up to 100 1x1 km tiles.
    Every tile within the batch follows the same download-and-validate process
    used for other AHN versions.

    The partition succeeds if at least one tile is downloaded or already on disk.
    Individual tile failures are logged as warnings and do not fail the batch.
    """
    batch_id = context.partition_key
    tiles = tiles_in_batch(batch_id)
    if not tiles:
        return Output(
            BatchLAZDownload(batch_id=batch_id, tiles={}),
            metadata={"batch": batch_id, "tiles": 0},
        )

    laz_dir = pointcloud_store.create_subdir("AHN6/as_downloaded/LAZ")
    total = len(tiles)

    batch_tiles: dict[str, LAZDownload] = {}
    downloaded = 0
    skipped = 0
    failed = 0
    completed = 0

    logger.info(f"Batch {batch_id}: starting {total} tiles")

    for tile_id in tiles:
        idx_entry = tile_index_ahn6.get(tile_id)
        if idx_entry is None or idx_entry.get("url") is None:
            logger.warning(f"  tile {tile_id}: not found in tile index")
            failed += 1
            completed += 1
            continue
        url = idx_entry["url"]
        fpath = laz_dir / url.split("/")[-1]
        completed += 1
        try:
            verify_ssl = False
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", category=urllib3.exceptions.InsecureRequestWarning
                )
                lazdownload = download_ahn_laz(
                    fpath=fpath,
                    url_laz=url,
                    verify_ssl=verify_ssl,
                    force_download=config.force_download,
                )
            lazdownload.compute_sha(HashChunkwise("sha256"))
            if config.check_hash:
                first_validation = lazdownload.validate(
                    sha_reference=sha256_ahn6, sha_func=HashChunkwise("sha256")
                )
                if not first_validation:
                    logger.info(
                        format_laz_log(
                            fpath,
                            "First validation failed. Removing and retrying...",
                        )
                    )
                    fpath.unlink()
                    with warnings.catch_warnings():
                        warnings.filterwarnings(
                            "ignore",
                            category=urllib3.exceptions.InsecureRequestWarning,
                        )
                        lazdownload = download_ahn_laz(
                            fpath=fpath,
                            url_laz=url,
                            verify_ssl=verify_ssl,
                        )
                    second_validation = lazdownload.validate(
                        sha_reference=sha256_ahn6, sha_func=HashChunkwise("sha256")
                    )
                    if not second_validation:
                        logger.warning(format_laz_log(fpath, "Checksum failed"))
                else:
                    logger.debug(format_laz_log(fpath, "Validation OK"))

            batch_tiles[tile_id] = lazdownload
            if lazdownload.new:
                downloaded += 1
                logger.info(
                    f"  [{completed}/{total}] {tile_id}: "
                    f"downloaded ({lazdownload.size:.1f} MB)"
                )
            else:
                skipped += 1
                logger.debug(f"  [{completed}/{total}] {tile_id}: already on disk")
        except Failure as exc:
            logger.warning(f"  [{completed}/{total}] {tile_id}: FAILED — {exc}")
            failed += 1

    if downloaded + skipped == 0:
        raise Failure(
            f"Batch {batch_id}: all {total} tiles failed. No COPC files on disk."
        )

    logger.info(
        f"Batch {batch_id}: done — "
        f"{downloaded} downloaded, {skipped} skipped, {failed} failed "
        f"of {total} tiles"
    )

    return Output(
        BatchLAZDownload(batch_id=batch_id, tiles=batch_tiles),
        metadata={
            "batch": batch_id,
            "tiles": total,
            "downloaded": downloaded,
            "skipped": skipped,
            "failed": failed,
        },
    )


def get_checksums(url_map: Mapping[int, str], ahn_version: int) -> dict[str, str]:
    """
    Get the AHN LAZ file checksums for the given AHN version.

    Args:
        url_map (Mapping[int, str]): A mapping between AHN versions as keys and
            their corresponding checksum file URL as values.
        ahn_version (int): The version of AHN.

    Returns:
        Mapping[str, str]: A dictionary where the keys are filenames and the values
            are their corresponding SHA-256 checksums.
    """
    url = url_map[ahn_version]
    _hashes = download_as_str(url)
    checksums = {}
    # We have a GeoJSON FeatureCollection
    for feature in json.loads(_hashes)["features"]:
        if (properties := feature.get("properties")) and (
            file_url := properties.get("file")
        ):
            filename = file_url.split("/")[-1]
            checksums[filename] = properties.get("sha256")
    return checksums


HEAD_CHECK_TIMEOUT = 10


def _is_http_url(url: str) -> bool:
    return isinstance(url, str) and url.startswith(("http://", "https://"))


def _head_check(url: str, fpath: Path, verify_ssl: bool = True) -> None:
    """Pre-check a LAZ download URL with a HEAD request.

    Raises :class:`dagster.Failure` (without retrying) when the URL cannot be
    used, i.e. the URL is empty/not an ``http(s)`` URL, the server returns
    403/404, or any network/timeout/HTTP error occurs. Returns normally only
    when the server answered positively, so the caller proceeds to the download.
    """
    if not _is_http_url(url):
        raise Failure(
            format_laz_log(
                fpath,
                "No valid download URL for this tile (the AHN LAZ URL is "
                "missing or malformed in the tile index)",
            )
        )
    try:
        resp = requests.head(
            url, timeout=HEAD_CHECK_TIMEOUT, allow_redirects=True, verify=verify_ssl
        )
    except (requests.RequestException, ValueError):
        raise Failure(
            format_laz_log(fpath, f"URL {url} is unreachable (network error)")
        )
    if resp.status_code in (403, 404):
        raise Failure(
            format_laz_log(
                fpath, f"URL returned HTTP {resp.status_code} (not retrying)"
            )
        )


def download_ahn_laz(
    fpath: Path,
    url_laz: str | None = None,
    url_base: str | None = None,
    verify_ssl: bool = False,
    nr_retries: int = 5,
    force_download: bool = False,
) -> LAZDownload:
    """Download an AHN LAZ file from the input url to the given path,
    if the file does not exists.

    Args:
        force_download: Force downloading the file even if it exists on disk.
        nr_retries: The number of retries to download the file.
        fpath: Path to the LAZ file that may exist locally. If not it will be downloaded.
        url_laz: Complete URL of the file to download. If provided, 'url_base' is
            ignored.
        url_base (str): Base URL for the file to be downloaded.
        verify_ssl (bool): Whether to verify the SSL certificate of the URL.

    Returns:
        A LAZDownload file
    """

    if url_laz is not None:
        url = url_laz
    elif url_base is not None:
        url = f"{url_base}/{fpath.name}"
    else:
        url = None

    # Pre-check the URL: raises Failure (without retry) for a missing/malformed
    # URL or an HTTP 403/404. For OK/UNREACHABLE we proceed to the real download,
    # which retries on transient errors.
    _head_check(url, fpath=fpath, verify_ssl=verify_ssl)

    success = False
    file_size = 0.0
    is_new = False
    if not fpath.is_file():
        logger.info(format_laz_log(fpath, "Not found locally. Downloading..."))
        file_size, fpath, is_new, success, url_laz = download_laz(  # type: ignore[assignment]
            file_size, fpath, is_new, nr_retries, success, url, url_laz, verify_ssl
        )
    else:  # pragma: no cover
        logger.info(format_laz_log(fpath, "File already downloaded"))
        success = True
        file_size = round(fpath.stat().st_size / 1e6, 2)
        is_new = False
        if force_download:
            logger.info(format_laz_log(fpath, "Forcing re-download"))
            file_size, fpath, is_new, success, url_laz = download_laz(  # type: ignore[assignment]
                file_size, fpath, is_new, nr_retries, success, url, url_laz, verify_ssl
            )

    if not success:
        raise Failure(format_laz_log(fpath, "Downloading failed!"))

    assert url_laz is not None
    assert isinstance(fpath, Path)
    return LAZDownload(
        url=url_laz,
        path=fpath,
        success=success,
        hash_name=None,
        hash_hexdigest=None,
        new=is_new,
        size=file_size,
    )


def download_laz(
    file_size, fpath, is_new, nr_retries, success, url, url_laz, verify_ssl
):
    if url is None:
        url_laz = None
        fpath_download = Path()
        success = False
        is_new = False
        file_size = 0.0
        logger.error("Cannot download from url with value None")
        return file_size, fpath, is_new, success, url_laz

    fpath_download = Path()
    for i in range(nr_retries):
        fpath_download = download_file(
            url=url,
            target_path=fpath,
            chunk_size=1024 * 1024,
            verify=verify_ssl,
            attempt_resume=True,
        )
        if fpath_download is None:
            # Download failed
            if i == nr_retries - 1:
                url_laz = None
                fpath_download = Path()
                success = False
                is_new = False
                file_size = 0.0
                logger.error(f"Download failed after {i + 1} retries")
            else:
                logger.warning(f"Retrying ({i + 1}/{nr_retries})")
                time.sleep(random.randrange(1, 5))
        else:
            success = True
            is_new = True
            file_size = round(fpath_download.stat().st_size / 1e6, 2)
            break

    return file_size, fpath_download, is_new, success, url_laz


def match_sha(
    fpath: Path, sha_reference: Mapping[str, str], hash_name: str, hash_hexdigest: str
) -> bool:
    """Verify the SHA of a file against a reference.

    Args:
        hash_hexdigest: The hexadecimal digest of the data passed through the hasher
        hash_name: Hash function name
        fpath: Path to the file
        sha_reference: Reference SHA sums to match against,
            as { filename : SHA }
    Returns:
        True on matching hashes
    """
    if not sha_reference[fpath.name]:
        # this check if for ensuring that new AHN5 tiles which do not have a
        # checksum yet will still be downloaded.
        logger.info(format_laz_log(fpath, f"{hash_name} doesn't have a hash"))
        return True
    elif hash_hexdigest == sha_reference[fpath.name]:
        logger.info(format_laz_log(fpath, f"{hash_name} OK"))
        return True
    else:  # pragma: no cover
        logger.info(
            format_laz_log(
                fpath, f"{hash_name}: {hash_hexdigest} != {sha_reference[fpath.name]}"
            )
        )
        return False
