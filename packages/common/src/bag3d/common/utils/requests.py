from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Mapping, Union
from urllib.parse import urlparse, urljoin

import requests
from dagster import (
    get_dagster_logger,
    RetryRequested,
    PathMetadataValue,
    UrlMetadataValue,
    FloatMetadataValue,
)


def download_as_str(url: str, parameters: Mapping = None) -> str:
    """Download a file as string in memory.

    Returns:
         The downloaded package as string.
    """
    resp = requests.get(url=url, params=parameters, verify=True)
    if resp.status_code == 200:
        return resp.text
    else:  # pragma: no cover
        raise ValueError(
            f"Failed to download JSON. HTTP Status {resp.status_code} for {resp.url}"
        )


def download_file(
    url: str,
    target_path: Path,
    chunk_size: int = 1024,
    parameters: Mapping = None,
    verify: bool = True,
) -> Union[Path, None]:
    """Download a large file and save it to disk.

    Args:
        url (str): The URL of the file to be downloaded.
        target_path (Path): Path to the target file or directory. If ``save_path`` is a
            directory, then the target file name is the last part of the ``url``.
        chunk_size (int): The ``chunk_size`` parameter passed to
            :py:func:`request.Response.iter_content. Defaults to ``1024``.
        parameters (dict): Query parameters passed to :py:func:`requests.get`.
        verify (bool): Whether to verify SSL certificate.
    Returns:
        The local Path to the downloaded file, or None on failure
    """
    logger = get_dagster_logger()
    if target_path.is_dir():
        local_filename = url.split("/")[-1]
        fpath = target_path / local_filename
    else:
        fpath = target_path
    logger.info(f"Downloading from {url} to {fpath}")
    session = requests.Session()  # https://stackoverflow.com/a/63417213

    try:
        r = session.get(url, params=parameters, stream=True, verify=verify)
        if r.ok:
            with fpath.open("wb") as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
            return fpath
        else:  # pragma: no cover
            r.raise_for_status()
        r.close()
    except (
        requests.exceptions.BaseHTTPError,
        requests.exceptions.HTTPError,
    ) as e:  # pragma: no cover
        logger.exception(e)
        return None


def get_metadata(url_api: str):
    """Get metadata from a PDOK API.

    :returns: {"timeliness": <date>: [featuretype,...]}
    """
    r_meta = requests.get(url_api, verify=True)
    if not r_meta.status_code == requests.codes.ok:  # pragma: no cover
        r_meta.raise_for_status()
    meta = {"timeliness": {}}
    for layer in r_meta.json()["timeliness"]:
        if layer["datetimeTo"][-1] == "Z":
            dt = str(datetime.fromisoformat(layer["datetimeTo"][:-1]).date())
        else:  # pragma: no cover
            dt = str(datetime.fromisoformat(layer["datetimeTo"]).date())
        if dt in meta["timeliness"]:
            meta["timeliness"][dt].append(layer["featuretype"])
        else:
            meta["timeliness"][dt] = [
                layer["featuretype"],
            ]
    return meta


def get_extract_download_link(url, featuretypes, data_format, geofilter) -> str:
    """Request an export and download link from the API."""
    logger = get_dagster_logger()
    request_json = {
        "featuretypes": featuretypes,
        "format": data_format,
    }
    if geofilter is not None:
        request_json["geofilter"] = geofilter
    r_post = requests.post(url, json=request_json)
    logger.info(f"Requesting extract: {r_post.url} with {request_json} ")
    if not r_post.status_code == requests.codes.accepted:  # pragma: no cover
        logger.error(r_post.text)
        r_post.raise_for_status()
    else:
        _u = urlparse(url)
        pdok_server = f"{_u.scheme}://{_u.hostname}/"
        url_status = urljoin(pdok_server, r_post.json()["_links"]["status"]["href"])
        url_download = None

        if requests.get(url_status, verify=True).status_code == requests.codes.ok:
            while (
                r_status := requests.get(url_status, verify=True)
            ).status_code == requests.codes.ok:
                sleep(15)
            if not r_status.status_code == requests.codes.created:  # pragma: no cover
                logger.error(r_status.text)
                r_status.raise_for_status()
            url_download = urljoin(
                pdok_server, r_status.json()["_links"]["download"]["href"]
            )
        elif (
            requests.get(url_status, verify=True).status_code == requests.codes.created
        ):
            r_status = requests.get(url_status, verify=True)
            url_download = urljoin(
                pdok_server, r_status.json()["_links"]["download"]["href"]
            )
        else:  # pragma: no cover
            _r = requests.get(url_status, verify=True)
            logger.error(_r.text)
            _r.raise_for_status()
        logger.info(f"Extract URL: {url_download}")
        return url_download


def download_extract(
    dataset, url_api, featuretypes, data_format, geofilter, download_dir
):
    _m = get_metadata(f"{url_api}/dataset")
    metadata = {"timeliness": {}}
    for dt, ft in _m["timeliness"].items():
        metadata["timeliness"][dt] = list(set(featuretypes).intersection(ft))

    # TODO: materialize if there is a newer version only https://docs.dagster.io/concepts/assets/software-defined-assets#conditional-materialization
    url_download = get_extract_download_link(
        url=f"{url_api}/full/custom",
        featuretypes=featuretypes,
        data_format=data_format,
        geofilter=geofilter,
    )

    dest_file = Path(download_dir) / f"{dataset}.zip"
    try:
        download_file(url=url_download, target_path=dest_file)
    except requests.HTTPError:
        raise RetryRequested(max_retries=2)

    return {
        "Extract Path": PathMetadataValue(dest_file),
        "Download URL": UrlMetadataValue(url_download),
        "Size [Mb]": FloatMetadataValue(
            round(dest_file.stat().st_size * 0.000001, ndigits=2)
        ),
        "timeliness": metadata["timeliness"],
    }
