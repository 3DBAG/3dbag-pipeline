import json
from pathlib import Path

_dir = Path(__file__).parent
AHN_TILE_IDS = set(json.loads((_dir / "ahn_tile_ids.json").read_text()))
AHN6_TILE_IDS = set(json.loads((_dir / "ahn6_tile_ids.json").read_text()))
