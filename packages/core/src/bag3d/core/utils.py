"""Small shared helpers for the core package."""

import hashlib
from pathlib import Path


def sha256_file(path: Path) -> str:
    """Streaming SHA-256 of a file, avoiding loading large archives into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
