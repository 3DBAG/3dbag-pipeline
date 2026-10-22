"""Durable manifests and content-based checkpoints, independent of Dagster."""

import fcntl
import hashlib
import json
import tempfile
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from pathlib import Path


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fingerprint(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def code_version() -> str:
    root = Path(__file__).parent
    return fingerprint(
        {str(p.relative_to(root)): digest_file(p) for p in sorted(root.rglob("*.py"))}
    )


def read_manifest(path: Path) -> dict:
    return json.loads(path.read_text())


def atomic_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    return path


def file_record(path: Path, **metadata) -> dict:
    return {
        "path": str(path.resolve()),
        "sha256": digest_file(path),
        "bytes": path.stat().st_size,
        **metadata,
    }


def valid_record(record: dict) -> bool:
    path = Path(record["path"])
    return (
        path.is_file()
        and path.stat().st_size == record["bytes"]
        and digest_file(path) == record["sha256"]
    )


@contextmanager
def exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def checkpoint(
    path: Path, inputs: object, produce: Callable[[], Iterable[Path]]
) -> list[dict]:
    """Publish a completion record only after every output has been produced.

    A changed/corrupt output invalidates the checkpoint. Outputs passed to the
    producer are owned by this work unit and may be replaced on retry.
    """
    signature = fingerprint(inputs)
    with exclusive_lock(path.with_suffix(".lock")):
        if path.exists():
            saved = read_manifest(path)
            if saved["signature"] == signature and all(
                valid_record(r) for r in saved["files"]
            ):
                return saved["files"]
        files = [file_record(p) for p in produce()]
        if not files:
            raise ValueError(f"Work unit produced no outputs: {path}")
        atomic_json(path, {"signature": signature, "files": files})
        return files


def save_manifest(path: Path, files: list[dict], **metadata) -> Path:
    return atomic_json(path, {"version": 1, "files": files, **metadata})


def verify_inputs(manifest: dict) -> None:
    for record in manifest["files"]:
        if not valid_record(record):
            raise ValueError(
                f"Input is missing or changed; rematerialize its asset: {record['path']}"
            )
