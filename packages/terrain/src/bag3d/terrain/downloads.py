"""Network acquisition only. Raster processing never calls this module."""

import json
import math
import re
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from urllib.parse import urljoin

import rasterio
import requests
from rasterio.warp import transform_bounds
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .state import (
    atomic_json,
    digest_file,
    exclusive_lock,
    file_record,
    fingerprint,
    read_manifest,
    save_manifest,
    valid_record,
)

DATA = Path(__file__).parent / "data"
GRID_NAMES = ("nl_nsgi_rdtrans2018.tif", "nl_nsgi_nlgeo2018.tif", "us_nga_egm08_25.tif")


def session() -> requests.Session:
    client = requests.Session()
    client.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
            )
        ),
    )
    return client


def validate_raster(path: Path, crs: int | None = None) -> None:
    with rasterio.open(path) as raster:
        if (
            not raster.count
            or not raster.crs
            or crs is not None
            and raster.crs.to_epsg() != crs
        ):
            raise ValueError(f"Unexpected raster CRS or bands: {path}")
        raster.read(1, window=((0, 1), (0, 1)))


def download(url: str, root: Path, crs: int | None = None) -> dict:
    """Revalidate cached downloads; never expose a partial file as an input.

    Cache directories are URL-specific; payloads are immutable and addressed by
    content hash. A changed remote payload cannot overwrite a running build's input.
    """
    cache = root / fingerprint(url)
    cache.mkdir(parents=True, exist_ok=True)
    state = cache / "download.json"
    with exclusive_lock(cache / ".lock"):
        previous = read_manifest(state) if state.exists() else None
        headers = {}
        if previous and valid_record(previous):
            if previous.get("etag"):
                headers["If-None-Match"] = previous["etag"]
            elif previous.get("last_modified"):
                headers["If-Modified-Since"] = previous["last_modified"]
        temporary = cache / "payload.part"
        try:
            with (
                session() as client,
                client.get(
                    url, headers=headers, stream=True, timeout=(30, 120)
                ) as response,
            ):
                if response.status_code == 304:
                    if not previous or not valid_record(previous):
                        raise ValueError(
                            f"Server returned 304 without a valid cached input: {url}"
                        )
                    return previous
                response.raise_for_status()
                with temporary.open("wb") as stream:
                    for chunk in response.iter_content(1024 * 1024):
                        stream.write(chunk)
                validate_raster(temporary, crs)
                payload = cache / f"{digest_file(temporary)}.tif"
                temporary.replace(payload)
                record = file_record(
                    payload,
                    url=url,
                    etag=response.headers.get("ETag"),
                    last_modified=response.headers.get("Last-Modified"),
                )
                atomic_json(state, record)
                return record
        finally:
            temporary.unlink(missing_ok=True)


def selection_bounds(bounds, radius_m):
    """Include interpolation neighbors and the source-transition warp buffer."""
    if bounds is None:
        return None
    west, south, east, north = transform_bounds(4326, 28992, *bounds)
    halo = radius_m + 300
    return transform_bounds(
        28992, 4326, west - halo, south - halo, east + halo, north + halo
    )


def ahn_collections(client, catalog_url: str) -> list[str]:
    """Resolve active item URLs rather than pinning replaceable layer UUIDs."""
    response = client.get(catalog_url, timeout=(30, 120))
    response.raise_for_status()
    collections = response.json()["collections"]
    if not collections:
        raise ValueError(f"AHN catalog returned no collections: {catalog_url}")
    urls = []
    for collection in collections:
        items = [
            link["href"] for link in collection["links"] if link.get("rel") == "items"
        ]
        if len(items) != 1:
            raise ValueError(f"Expected one items link in AHN collection: {collection}")
        urls.append(urljoin(response.url, items[0]))
    return urls


def ahn_catalog(root: Path, bounds, radius_m: float) -> Path:
    specification = json.loads((DATA / "ahn5m.json").read_text())
    selection = selection_bounds(bounds, radius_m)
    entries = {}
    with session() as client:
        collections = [
            collection
            for catalog_url in specification["catalogs"]
            for collection in ahn_collections(client, catalog_url)
        ]
        for collection in collections:
            url = collection
            params = (
                {"bbox": ",".join(str(v) for v in selection)} if selection else None
            )
            visited = set()
            while url:
                if url in visited:
                    raise ValueError(f"Cyclic AHN pagination: {url}")
                visited.add(url)
                response = client.get(url, params=params, timeout=(30, 120))
                response.raise_for_status()
                payload = response.json()
                features = payload["features"]
                # Ellipsis emits a next link back to page one on its empty final
                # page (and a placeholder numberMatched). Empty means complete;
                # retain cycle detection for nonempty pages that really loop.
                if not features:
                    break
                for feature in features:
                    source = feature["properties"][specification["property"]]
                    if not isinstance(source, str) or not source.startswith("https://"):
                        raise ValueError(f"Invalid AHN DTM URL: {source!r}")
                    entries[source] = {"url": source, "collection": collection}
                following = [
                    link["href"]
                    for link in payload.get("links", [])
                    if link.get("rel") == "next"
                ]
                url = urljoin(response.url, following[0]) if following else None
                params = None
    if not entries:
        raise ValueError("The AHN 5 m catalog returned no input rasters")
    catalog = {
        "version": 1,
        "files": [],
        "sources": [entries[k] for k in sorted(entries)],
        "requested_bounds": bounds,
        "selection_bounds": selection,
        "selection_radius_m": radius_m,
    }
    return atomic_json(root / "catalogs" / f"{fingerprint(catalog)}.json", catalog)


def download_ahn(root: Path, catalog_path: Path, workers: int) -> Path:
    catalog = read_manifest(catalog_path)
    with ThreadPoolExecutor(workers) as executor:
        files = list(
            executor.map(
                partial(download, root=root / "downloads/ahn"),
                [s["url"] for s in catalog["sources"]],
            )
        )
    bounds = None
    for record in files:
        with rasterio.open(record["path"]) as raster:
            if raster.crs.to_epsg() not in (28992, 7415):
                raise ValueError(f"Expected RD New AHN input: {record['path']}")
            if any(abs(v - 5) > 0.01 for v in raster.res):
                raise ValueError(f"Expected 5 m AHN input: {record['path']}")
            current = transform_bounds(raster.crs, 4326, *raster.bounds)
            bounds = (
                current
                if bounds is None
                else (
                    min(bounds[0], current[0]),
                    min(bounds[1], current[1]),
                    max(bounds[2], current[2]),
                    max(bounds[3], current[3]),
                )
            )
    output_bounds = catalog["requested_bounds"] or bounds
    manifest_id = fingerprint(
        {
            "files": [(f["url"], f["sha256"]) for f in files],
            "bounds": output_bounds,
            "selection_radius_m": catalog["selection_radius_m"],
        }
    )
    return save_manifest(
        root / "manifests" / f"ahn-{manifest_id}.json",
        files,
        bounds=output_bounds,
        requested_bounds=catalog["requested_bounds"],
        selection_radius_m=catalog["selection_radius_m"],
    )


def download_grids(root: Path, workers: int) -> Path:
    with ThreadPoolExecutor(min(workers, 3)) as executor:
        files = list(
            executor.map(
                partial(download, root=root / "downloads/grids"),
                [f"https://cdn.proj.org/{name}" for name in GRID_NAMES],
            )
        )
    for record, name in zip(files, GRID_NAMES):
        record["name"] = name
    return save_manifest(
        root / "manifests" / f"grids-{fingerprint([f['sha256'] for f in files])}.json",
        files,
    )


def download_glo30(root: Path, ahn_manifest: Path, workers: int) -> Path:
    bounds = read_manifest(ahn_manifest)["bounds"]
    west, south, east, north = bounds
    if not (0 <= west < east < 10 and 49 <= south < north < 56):
        raise ValueError(
            "AHN bounds extend beyond the bundled Netherlands GLO-30 catalog"
        )
    catalog = {}
    for url in (DATA / "glo30-nl.txt").read_text().splitlines():
        match = re.search(r"_N(\d+)_00_E(\d+)_00_DEM", url)
        if match:
            catalog[int(match[1]), int(match[2])] = url
    cells = [
        {"lat": lat, "lon": lon, "url": catalog.get((lat, lon))}
        for lat in range(math.floor(south), math.ceil(north))
        for lon in range(math.floor(west), math.ceil(east))
    ]
    with ThreadPoolExecutor(min(workers, 4)) as executor:
        files = list(
            executor.map(
                partial(download, root=root / "downloads/glo30", crs=4326),
                [c["url"] for c in cells if c["url"]],
            )
        )
    # An absent catalog entry is an ocean cell in this NL inventory. A failed
    # request above raises; it must never silently become an ocean cell.
    manifest_id = fingerprint(
        {"cells": cells, "files": [f["sha256"] for f in files], "bounds": bounds}
    )
    return save_manifest(
        root / "manifests" / f"glo30-{manifest_id}.json",
        files,
        cells=cells,
        bounds=bounds,
        catalog_sha256=digest_file(DATA / "glo30-nl.txt"),
    )
