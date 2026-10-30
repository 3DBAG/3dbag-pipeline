from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pytest
import requests

from bag3d.terrain import downloads
from bag3d.terrain.state import read_manifest, save_manifest


class Response:
    def __init__(
        self,
        payload=None,
        body=b"",
        status=200,
        headers=None,
        url="https://catalog.example/items",
    ):
        self.payload, self.body, self.status_code = payload, body, status
        self.headers, self.url = headers or {}, url

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self.payload

    def iter_content(self, size):
        yield self.body


def mock_session(monkeypatch, replies):
    calls = []

    class Client:
        def get(self, url, **kwargs):
            calls.append((url, kwargs))
            return replies.pop(0)

    @contextmanager
    def session():
        yield Client()

    monkeypatch.setattr(downloads, "session", session)
    return calls


def feature(url):
    return {"properties": {"Maaiveldmodel (DTM) 5m": url}}


def collection_response(items_url):
    return Response({"collections": [{"links": [{"rel": "items", "href": items_url}]}]})


def catalog_responses():
    return [
        collection_response("https://catalog.example/items"),
        collection_response("https://catalog.example/ahn6/current/items"),
    ]


def test_catalog_pagination_deduplication_and_interpolation_halo(tmp_path, monkeypatch):
    replies = catalog_responses() + [
        Response(
            {
                "features": [feature("https://source/a.tif")],
                "links": [{"rel": "next", "href": "?page=2"}],
            }
        ),
        Response(
            {
                "features": [
                    feature("https://source/a.tif"),
                    feature("https://source/b.tif"),
                ]
            }
        ),
        Response({"features": []}),
    ]
    calls = mock_session(monkeypatch, replies)
    bounds = [4.3, 52, 4.4, 52.1]
    catalog = read_manifest(downloads.ahn_catalog(tmp_path, bounds, 500))
    assert [r["url"] for r in catalog["sources"]] == [
        "https://source/a.tif",
        "https://source/b.tif",
    ]
    assert len(calls) == 5
    assert calls[3][0] == "https://catalog.example/items?page=2"
    assert calls[3][1]["params"] is None
    assert catalog["selection_bounds"][0] < bounds[0]
    assert catalog["selection_bounds"][2] > bounds[2]


def test_empty_final_page_ignores_broken_next_link_and_keeps_other_collection(
    tmp_path, monkeypatch
):
    replies = catalog_responses() + [
        Response(
            {
                "features": [feature("https://source/ahn5.tif")],
                "links": [{"rel": "next", "href": "?offset=last"}],
            }
        ),
        Response(
            {
                "features": [],
                "numberMatched": 999999999,
                "numberReturned": 0,
                "links": [{"rel": "next", "href": "https://catalog.example/items"}],
            }
        ),
        Response({"features": [feature("https://source/ahn6.tif")]}),
    ]
    calls = mock_session(monkeypatch, replies)
    catalog = read_manifest(
        downloads.ahn_catalog(tmp_path, [4.35, 52, 4.38, 52.025], 500)
    )
    assert [source["url"] for source in catalog["sources"]] == [
        "https://source/ahn5.tif",
        "https://source/ahn6.tif",
    ]
    assert len(calls) == 5
    assert calls[-1][0] == "https://catalog.example/ahn6/current/items"
    assert catalog["sources"][1]["collection"] == calls[-1][0]


def test_initial_empty_collection_does_not_follow_its_next_link(tmp_path, monkeypatch):
    replies = catalog_responses() + [
        Response(
            {
                "features": [],
                "links": [{"rel": "next", "href": "https://catalog.example/items"}],
            }
        ),
        Response({"features": [feature("https://source/ahn6.tif")]}),
    ]
    calls = mock_session(monkeypatch, replies)
    catalog = read_manifest(downloads.ahn_catalog(tmp_path, None, 500))
    assert len(catalog["sources"]) == 1
    assert len(calls) == 4


def test_unavailable_dtm_entries_keep_pagination_and_other_collection(
    tmp_path, monkeypatch, caplog
):
    replies = catalog_responses() + [
        Response({"features": [feature("https://source/ahn5.tif")]}),
        Response(
            {
                "features": [feature(""), feature(None)],
                "links": [{"rel": "next", "href": "?page=2"}],
            },
            url="https://catalog.example/ahn6/current/items",
        ),
        Response({"features": [feature("https://source/ahn6.tif")]}),
    ]
    calls = mock_session(monkeypatch, replies)
    catalog = read_manifest(downloads.ahn_catalog(tmp_path, None, 500))
    assert [source["url"] for source in catalog["sources"]] == [
        "https://source/ahn5.tif",
        "https://source/ahn6.tif",
    ]
    assert len(calls) == 5
    assert calls[-1][0] == "https://catalog.example/ahn6/current/items?page=2"
    assert "Skipped 2 AHN entries without a 5 m DTM URL" in caplog.text
    assert "https://catalog.example/ahn6/current/items" in caplog.text


def test_only_unavailable_dtms_does_not_publish_catalog(tmp_path, monkeypatch):
    mock_session(
        monkeypatch,
        catalog_responses()
        + [
            Response({"features": [feature("")]}),
            Response({"features": [feature(None)]}),
        ],
    )
    with pytest.raises(ValueError, match="returned no input rasters"):
        downloads.ahn_catalog(tmp_path, None, 500)
    assert not list((tmp_path / "catalogs").glob("*.json"))


@pytest.mark.parametrize("source", ["http://source/tile.tif", "not a URL", 0, False])
def test_invalid_dtm_url_still_fails(tmp_path, monkeypatch, source):
    mock_session(
        monkeypatch,
        catalog_responses() + [Response({"features": [feature(source)]})],
    )
    with pytest.raises(ValueError, match="Invalid AHN DTM URL"):
        downloads.ahn_catalog(tmp_path, None, 500)
    assert not list((tmp_path / "catalogs").glob("*.json"))


def test_nonempty_pagination_cycle_still_fails(tmp_path, monkeypatch):
    replies = catalog_responses() + [
        Response(
            {
                "features": [feature("https://source/a.tif")],
                "links": [{"rel": "next", "href": "https://catalog.example/items"}],
            }
        ),
    ]
    mock_session(monkeypatch, replies)
    with pytest.raises(ValueError, match="Cyclic AHN pagination"):
        downloads.ahn_catalog(tmp_path, None, 500)
    assert not list((tmp_path / "catalogs").glob("*.json"))


def test_collection_discovery_resolves_current_relative_items_link(monkeypatch):
    mock_session(
        monkeypatch, [collection_response("./collections/replacement-layer/items")]
    )
    with downloads.session() as client:
        assert downloads.ahn_collections(
            client, "https://catalog.example/collections"
        ) == ["https://catalog.example/collections/replacement-layer/items"]


def test_download_is_atomic_revalidated_and_content_addressed(
    tmp_path, write_raster, monkeypatch
):
    first = write_raster(tmp_path / "first.tif", np.ones((8, 8))).read_bytes()
    second = write_raster(tmp_path / "second.tif", np.full((8, 8), 2)).read_bytes()
    replies = [
        Response(body=first, headers={"ETag": "one"}),
        Response(status=304),
        Response(body=second, headers={"ETag": "two"}),
    ]
    calls = mock_session(monkeypatch, replies)
    root = tmp_path / "downloads"
    original = downloads.download("https://source/tile.tif", root)
    assert downloads.download("https://source/tile.tif", root) == original
    changed = downloads.download("https://source/tile.tif", root)
    assert calls[1][1]["headers"] == {"If-None-Match": "one"}
    assert changed["path"] != original["path"]
    assert Path(original["path"]).read_bytes() == first
    assert not list(root.rglob("*.part"))


def test_bad_download_does_not_publish_input(tmp_path, monkeypatch):
    mock_session(monkeypatch, [Response(body=b"not a raster")])
    with pytest.raises(Exception, match="supported file format"):
        downloads.download("https://source/tile.tif", tmp_path)
    assert not list(tmp_path.rglob("download.json"))
    assert not list(tmp_path.rglob("*.part"))


def test_glo_download_failure_never_becomes_ocean(tmp_path, monkeypatch):
    manifest = save_manifest(tmp_path / "ahn.json", [], bounds=[4.2, 52.1, 4.3, 52.2])
    mock_session(monkeypatch, [Response(status=404)])
    with pytest.raises(requests.HTTPError):
        downloads.download_glo30(tmp_path, manifest, 1)
    assert not list((tmp_path / "manifests").glob("glo30-*.json"))


def test_confirmed_ocean_needs_no_network(tmp_path, monkeypatch):
    manifest = save_manifest(tmp_path / "ahn.json", [], bounds=[3.2, 53.1, 3.3, 53.2])
    mock_session(monkeypatch, [])
    result = read_manifest(downloads.download_glo30(tmp_path, manifest, 1))
    assert result["files"] == []
    assert result["cells"] == [{"lat": 53, "lon": 3, "url": None}]
