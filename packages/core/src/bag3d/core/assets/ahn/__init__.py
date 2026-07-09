import gzip
import json
from pathlib import Path

_dir = Path(__file__).parent
AHN_TILE_IDS = set(
    json.loads(gzip.decompress((_dir / "ahn_tile_ids.json.gz").read_bytes()))
)
AHN6_TILE_IDS = set(
    json.loads(gzip.decompress((_dir / "ahn6_tile_ids.json.gz").read_bytes()))
)
