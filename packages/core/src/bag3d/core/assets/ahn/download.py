import json
import time
import random
import warnings
from pathlib import Path
from typing import Any, Mapping, Union, Optional
from hashlib import new as hash_new, algorithms_available
from dataclasses import dataclass
import urllib.request
import urllib.error

from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib3

from dagster import (
    asset,
    Output,
    get_dagster_logger,
    Config,
    Failure,
    AssetExecutionContext,
    AutomationCondition,
)

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.utils.requests import download_file, download_as_str
from bag3d.core.assets.ahn.core import (
    format_laz_log,
    download_ahn_index,
    partition_definition_ahn,
    partition_definition_km_batches,
    tiles_in_batch,
)

logger = get_dagster_logger("ahn.download")

# AHN LAZ file with checksums.
URL_LAZ_SHA = {
    6: None,
    5: "https://fsn1.your-objectstorage.com/hwh-portal/20230609_tmp/links/nationaal/Nederland/AHN5_PC.json",
    4: "https://gist.githubusercontent.com/fwrite/6bb4ad23335c861f9f3162484e57a112/raw/ee5274c7c6cf42144d569e303cf93bcede3e2da1/AHN4.md5",
    3: "https://gist.githubusercontent.com/arbakker/dcca00384cddbdf10c0421ed26d8911c/raw/f43465d287a654254e21851cce38324eba75d03c/checksum_laz.md5",
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


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def md5_ahn3() -> dict[str, str]:
    """Download the MD5 sums that are calculated by PDOK for the AHN3 LAZ files."""
    return get_checksums(URL_LAZ_SHA, ahn_version=3)


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def md5_ahn4() -> dict[str, str]:
    """Download the MD5 sums that are calculated by PDOK for the AHN4 LAZ files."""
    return get_checksums(URL_LAZ_SHA, ahn_version=4)


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def sha256_ahn5() -> dict[str, str]:
    """Download the SHA256 sums for the AHN5 LAZ files, provided by AHN."""
    return get_checksums(URL_LAZ_SHA, ahn_version=5)


@asset(automation_condition=AutomationCondition.on_cron("0 0 1 * *"))
def tile_index_ahn() -> dict[str, dict[str, Any] | None] | None:
    """The AHN tile index, including the tile geometry and the file download links."""
    return download_ahn_index(with_geom=True)


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
    md5_ahn3,
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
    lazdownload.compute_sha(HashChunkwise("md5"))
    if config.check_hash:
        first_validation = lazdownload.validate(
            sha_reference=md5_ahn3, sha_func=HashChunkwise("md5")
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
                sha_reference=md5_ahn3, sha_func=HashChunkwise("md5")
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
    md5_ahn4,
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
    lazdownload.compute_sha(HashChunkwise("md5"))
    if config.check_hash:
        first_validation = lazdownload.validate(
            sha_reference=md5_ahn4, sha_func=HashChunkwise("md5")
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
                sha_reference=md5_ahn4, sha_func=HashChunkwise("md5")
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
    sha256_ahn5,
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
    lazdownload.compute_sha(HashChunkwise("md5"))
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


def get_checksums(url_map: Mapping[int, str], ahn_version: int) -> dict[str, str]:
    """
    Get the AHN LAZ file checksums for the given AHN version.

    Args:
        url_map (Mapping[int, str]): A mapping between AHN versions as keys and
            their corresponding checksum file URL as values.
        ahn_version (int): The version of AHN.

    Returns:
        Mapping[str, str]: A dictionary where the keys are filenames and the values
            are their corresponding SHA-256 or MD5 checksums.
    """
    url = url_map[ahn_version]
    _hashes = download_as_str(url)
    checksums = {}
    if ahn_version == 5:
        # We have a GeoJSON FeatureCollection
        for feature in json.loads(_hashes)["features"]:
            if properties := feature.get("properties"):
                if file_url := properties.get("file"):
                    filename = file_url.split("/")[-1]
                    checksums[filename] = properties.get("sha256")
    else:
        for tile in _hashes.strip().split("\n"):
            sha, file = tile.split()
            checksums[file] = sha
    return checksums


def _head_check(url: str) -> Optional[int]:
    """Quick HEAD check. Returns HTTP status code, or None on network error."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except (urllib.error.URLError, OSError, TimeoutError):
        return None


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
    else:
        assert url_base is not None, "Either url_laz or url_base must be provided"
        url = "/".join([url_base, fpath.name])

    http_status = _head_check(url)
    if http_status in (403, 404):
        raise Failure(
            format_laz_log(fpath, f"URL returned HTTP {http_status} (not retrying)")
        )

    success = False
    file_size = 0.0
    is_new = False
    if not fpath.is_file():
        logger.info(format_laz_log(fpath, "Not found. Downloading..."))
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
        logger.info(format_laz_log(fpath, f"{hash_name} mismatch"))
        return False


# ---------------------------------------------------------------------------
# KM (1×1 km) grid COPC download assets
# URL templates derived from:
#   https://basisdata.nl/hwh-ahn/AUX/bladwijzer/index.html
# ---------------------------------------------------------------------------

BASE_URL = "https://basisdata.nl/hwh-ahn"

COPC_URL_TEMPLATES: dict[int, list[str]] = {
    6: [
        f"{BASE_URL}/AHN6/01_LAZ/AHN6_2025_C_{{tile}}.COPC.LAZ",
        f"{BASE_URL}/AHN6_KM/01_LAZ/AHN6_C_{{tile}}.COPC.LAZ",
        f"{BASE_URL}/AHN6/01_LAZ/AHN6_2025_C_{{tile}}.LAZ",
        f"{BASE_URL}/AHN6/01_LAZ/AHN6_C_{{tile}}.LAZ",
    ],
}


def probe_copc_url(url: str) -> Optional[int]:
    """Check if a COPC/LAZ URL exists via HEAD request.

    Returns:
        Content-Length in bytes if 200, None otherwise.
    """
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                content_length = resp.headers.get("Content-Length")
                return int(content_length) if content_length else 0
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        pass
    return None


class CopcFilesConfig(Config):
    force_download: bool = False


def resolve_copc_url(version: int, tile_id: str) -> Optional[str]:
    """Find the first working COPC/LAZ URL for a given AHN version and tile.

    Tries each URL template in order. Returns the first URL that responds
    with HTTP 200, or None if no pattern works.
    """
    for template in COPC_URL_TEMPLATES[version]:
        url = template.format(tile=tile_id)
        if probe_copc_url(url) is not None:
            return url
    return None


def _download_one_tile(
    tile_id: str,
    version: int,
    templates: list[str],
    laz_dir: Path,
    force_download: bool,
) -> tuple[str, str, float, bool]:
    """Download a single COPC/LAZ tile. Returns (tile_id, url, size_mb, new)."""
    last_error = None
    for template in templates:
        url = template.format(tile=tile_id)
        fpath = laz_dir / url.split("/")[-1]
        if fpath.is_file() and not force_download:
            size = round(fpath.stat().st_size / 1e6, 2)
            return tile_id, url, size, False

        verify_ssl = False
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", category=urllib3.exceptions.InsecureRequestWarning
            )
            try:
                lazdownload = download_ahn_laz(
                    fpath=fpath,
                    url_laz=url,
                    verify_ssl=verify_ssl,
                    force_download=force_download,
                )
                return tile_id, lazdownload.url, lazdownload.size, lazdownload.new
            except Failure:
                last_error = str(url)
                continue

    raise Failure(
        f"AHN{version} tile {tile_id}: download failed "
        f"(tried {len(templates)} URLs, last: {last_error})"
    )


def _make_copc_asset(version: int):
    """Factory to create batched COPC download assets for a given AHN version.

    Each partition is a 10x10 km block containing up to 100 1x1 km tiles.
    Tiles within a batch are downloaded in parallel using a thread pool.

    The partition succeeds if at least one tile is downloaded or already on disk.
    Individual tile failures are logged as warnings and do not fail the batch.
    """

    @asset(
        name=f"laz_files_ahn{version}_km",
        partitions_def=partition_definition_km_batches,
        pool="laz_download",
    )
    def _asset(
        context: AssetExecutionContext,
        config: CopcFilesConfig,
        pointcloud_store: FileStoreResource,
    ) -> Output[dict]:
        batch_id = context.partition_key
        tiles = tiles_in_batch(batch_id)
        if not tiles:
            return Output({}, metadata={"batch": batch_id, "tiles": 0})

        templates = COPC_URL_TEMPLATES[version]
        laz_dir = pointcloud_store.create_subdir(f"AHN{version}/as_downloaded/LAZ")
        total = len(tiles)

        results: dict[str, dict] = {}
        downloaded = 0
        skipped = 0
        failed = 0
        completed = 0

        logger.info(f"Batch {batch_id} (AHN{version}): starting {total} tiles")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(
                    _download_one_tile,
                    tile_id,
                    version,
                    templates,
                    laz_dir,
                    config.force_download,
                ): tile_id
                for tile_id in tiles
            }
            for future in as_completed(futures):
                tile_id = futures[future]
                completed += 1
                try:
                    tid, url, size, is_new = future.result()
                    results[tile_id] = {"url": url, "size_mb": size, "new": is_new}
                    if is_new:
                        downloaded += 1
                        logger.info(
                            f"  [{completed}/{total}] {tile_id}: downloaded ({size:.1f} MB)"
                        )
                    else:
                        skipped += 1
                        logger.debug(
                            f"  [{completed}/{total}] {tile_id}: already on disk"
                        )
                except Failure as exc:
                    logger.warning(f"  [{completed}/{total}] {tile_id}: FAILED — {exc}")
                    results[tile_id] = {"error": str(exc)}
                    failed += 1

        if downloaded + skipped == 0:
            raise Failure(
                f"Batch {batch_id}: all {total} tiles failed. No COPC files on disk."
            )

        logger.info(
            f"Batch {batch_id} (AHN{version}): done — "
            f"{downloaded} downloaded, {skipped} skipped, {failed} failed "
            f"of {total} tiles"
        )

        return Output(
            results,
            metadata={
                "batch": batch_id,
                "tiles": total,
                "downloaded": downloaded,
                "skipped": skipped,
                "failed": failed,
            },
        )

    return _asset


laz_files_ahn6_km = _make_copc_asset(6)
