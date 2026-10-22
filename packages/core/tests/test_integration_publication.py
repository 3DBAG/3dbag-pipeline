import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from zipfile import ZipFile

import pytest

from bag3d.core.assets.fixture_adapters import validate_fixture_manifest
from bag3d.core.assets.integration_data import (
    _archive_integration_data,
    _publish_integration_data,
)


def _fixture_snapshot(root: Path) -> Path:
    root.mkdir()
    fixture_file = root / "fixture.dat"
    fixture_file.write_bytes(b"fixture")
    manifest = {
        "fixture_version": "2",
        "snapshot_id": "2026-10-22-test-run",
        "aoi": {
            "wkt": "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            "minx": 0,
            "miny": 0,
            "maxx": 1,
            "maxy": 1,
        },
        "clipping": {"vectors": "gdal", "pointclouds": "las2las_keep_xy"},
        "validation": {
            "buffer_metres": 10.0,
            "extents": {"vectors": {}, "pointclouds": {}},
        },
        "sources": {
            name: {"path": "fixture.dat"}
            for name in (
                "bag",
                "bgt",
                "top10nl",
                "cbs_key_figures",
                "cbs_buurtkaart",
            )
        },
        "ahn": {
            "indexes": {"3": {}, "6": {}},
            "checksums": {"3": {}, "4": {}, "5": {}, "6": {}},
            "partitions": {},
        },
        "files": {
            "fixture.dat": {
                "size": fixture_file.stat().st_size,
                "sha256": hashlib.sha256(fixture_file.read_bytes()).hexdigest(),
            }
        },
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


class _FakeConnection:
    def __init__(self, fail_upload: int | None = None):
        self.events = []
        self.fail_upload = fail_upload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def run(self, command: str, **kwargs):
        self.events.append(("run", command))

    def put(self, local_path: Path, remote_path: str):
        self.events.append(("put", Path(local_path), remote_path))
        upload_count = sum(event[0] == "put" for event in self.events)
        if upload_count == self.fail_upload:
            raise RuntimeError("upload failed")


class _FakePublicationServer:
    def __init__(self, connection: _FakeConnection):
        self._connection = connection

    @property
    @contextmanager
    def connection(self):
        yield self._connection


def test_archive_has_manifest_at_root_and_validates_after_extraction(tmp_path):
    fixture_root = tmp_path / "fixture-root"
    manifest_path = _fixture_snapshot(fixture_root)
    archive_path = _archive_integration_data(manifest_path)
    assert archive_path.parent != fixture_root

    with ZipFile(archive_path) as archive:
        assert "manifest.json" in archive.namelist()
        assert "fixture.dat" in archive.namelist()
        archive.extractall(tmp_path / "extracted")

    validated = validate_fixture_manifest(tmp_path / "extracted")
    assert validated.value["snapshot_id"] == "2026-10-22-test-run"
    assert validated.value["fixture_version"] == "2"


def test_publication_promotes_directory_after_both_uploads(tmp_path):
    manifest_path = _fixture_snapshot(tmp_path / "fixture-root")
    archive_path = tmp_path / "integration-data.zip"
    archive_path.write_bytes(b"archive")
    connection = _FakeConnection()

    _publish_integration_data(
        archive_path, manifest_path, _FakePublicationServer(connection)
    )

    uploads = [event for event in connection.events if event[0] == "put"]
    assert [event[2] for event in uploads] == [
        "/srv/data/integration-data/.2026-10-22-test-run.staging/integration-data.zip",
        "/srv/data/integration-data/.2026-10-22-test-run.staging/manifest.json",
    ]
    promotion_index = next(
        index
        for index, event in enumerate(connection.events)
        if event[0] == "run" and "mv --" in event[1]
    )
    assert promotion_index > max(
        index for index, event in enumerate(connection.events) if event[0] == "put"
    )


@pytest.mark.parametrize("failed_upload", [1, 2])
def test_failed_upload_leaves_version_unpublished(tmp_path, failed_upload):
    manifest_path = _fixture_snapshot(tmp_path / "fixture-root")
    archive_path = tmp_path / "integration-data.zip"
    archive_path.write_bytes(b"archive")
    connection = _FakeConnection(fail_upload=failed_upload)

    with pytest.raises(RuntimeError, match="upload failed"):
        _publish_integration_data(
            archive_path, manifest_path, _FakePublicationServer(connection)
        )

    assert not any(
        event[0] == "run" and "mv --" in event[1] for event in connection.events
    )
    assert any(
        event[0] == "run" and "rm -rf --" in event[1] for event in connection.events
    )
