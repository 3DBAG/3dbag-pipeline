from unittest.mock import patch

import pytest
import requests

pytest_plugins = ["bag3d.common.testing.conftest_plugin"]


class _MockResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        text: str = "",
        body: bytes = b"",
        json_data=None,
        headers: dict[str, str] | None = None,
        url: str = "",
    ):
        self.status_code = status_code
        self.text = text
        self._body = body
        self._json_data = json_data
        self.headers = headers or {}
        self.url = url

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code} for {self.url}")

    def iter_content(self, chunk_size: int = 1024):
        for start in range(0, len(self._body), chunk_size):
            yield self._body[start : start + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _MockSession:
    def __init__(self, registry: "_RequestsRegistry"):
        self._registry = registry

    def head(self, url: str, **kwargs):
        return self._registry._build_response("HEAD", url)

    def get(self, url: str, **kwargs):
        return self._registry._build_response("GET", url)


class _RequestsRegistry:
    def __init__(self):
        self._responses: dict[tuple[str, str], _MockResponse] = {}

    def get(
        self,
        url: str,
        *,
        text: str = "",
        body: bytes = b"",
        json=None,
        headers: dict[str, str] | None = None,
        status_code: int = 200,
    ) -> None:
        self._responses[("GET", url)] = _MockResponse(
            status_code=status_code,
            text=text,
            body=body,
            json_data=json,
            headers=headers,
            url=url,
        )

    def head(
        self, url: str, *, headers: dict[str, str] | None = None, status_code: int = 200
    ) -> None:
        self._responses[("HEAD", url)] = _MockResponse(
            status_code=status_code,
            headers=headers,
            url=url,
        )

    def _build_response(self, method: str, url: str) -> _MockResponse:
        response = self._responses[(method, url)]
        return _MockResponse(
            status_code=response.status_code,
            text=response.text,
            body=response._body,
            json_data=response._json_data,
            headers=response.headers,
            url=url,
        )


@pytest.fixture
def mock_requests():
    registry = _RequestsRegistry()

    def mock_get(url: str, **kwargs):
        return registry._build_response("GET", url)

    with (
        patch("bag3d.common.utils.requests.requests.get", side_effect=mock_get),
        patch(
            "bag3d.common.utils.requests.requests.Session",
            side_effect=lambda: _MockSession(registry),
        ),
    ):
        yield registry


@pytest.fixture(scope="function")
def wkt_testarea():
    """A small test area in the oldtown of Utrecht, incl. the Oudegracht."""
    yield "Polygon ((136251.531 456118.126, 136620.128 456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 456118.126))"
