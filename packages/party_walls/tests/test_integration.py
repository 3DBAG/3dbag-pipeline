"""Integration test for the building_surfaces asset with real CityJSONFeature data.

Runs the real shared_walls() computation (not the conftest stub) against 415 buildings
from tile 10/564/624. Adjacency is computed on-the-fly from LoD 0 footprints extracted
from the CityJSONFeature files using Shapely, without requiring a database or .gpkg files.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Detect whether the real building_surfaces library is installed.
#
# The conftest.py replaces sys.modules["building_surfaces.walls"] with a no-op
# stub at collection time.  Remove it so the real package can be imported for
# this check — but do NOT touch bag3d.party_walls.assets.*, because that would
# create a stale-module-reference bug in test_data_flow.py: functions already
# imported from the asset module keep their __globals__ pointing at the old
# module dict, while monkeypatch would patch the freshly reimported one.
# ---------------------------------------------------------------------------
for _mod in list(sys.modules):
    if _mod.startswith("building_surfaces"):
        del sys.modules[_mod]

_real_shared_walls: object = None
_real_write_cityjsonfeature: object = None
try:
    from building_surfaces.walls import (  # noqa: E402
        shared_walls as _real_shared_walls,
        write_cityjsonfeature as _real_write_cityjsonfeature,
    )

    _HAS_BUILDING_SURFACES = True
except ImportError:
    _HAS_BUILDING_SURFACES = False

pytestmark = pytest.mark.skipif(
    not _HAS_BUILDING_SURFACES, reason="building_surfaces not installed"
)

# Common imports that don't depend on building_surfaces
from bag3d.common.resources import nl_transform  # noqa: E402
from bag3d.common.resources.files import FileStoreResource  # noqa: E402
from bag3d.common.testing import build_asset_context_for  # noqa: E402

# ---------------------------------------------------------------------------
# Test data paths
# ---------------------------------------------------------------------------

_TEST_DATA = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "test_data"
    / "integration_party_walls"
)
_CROP_RECONSTRUCT = _TEST_DATA / "file_store_fastssd" / "3DBAG" / "crop_reconstruct"
_OBJECTS_DIR = _CROP_RECONSTRUCT / "10" / "564" / "624" / "objects"
_NL_TRANSFORM = {"scale": [0.001, 0.001, 0.001], "translate": [171800.0, 472700.0, 0.0]}


# ---------------------------------------------------------------------------
# Adjacency computation from LoD 0 footprints
# ---------------------------------------------------------------------------


def _build_adjacency_from_features(
    objects_dir: Path, transform: dict
) -> list[dict[str, str]]:
    """Extract LoD 0 footprints from CityJSONFeature files and compute adjacency.

    Uses the Building object's LoD 0 geometry (2D footprint) from each feature file.
    Two buildings are adjacent when their footprints intersect within a 0.1m buffer,
    matching the logic used to build reconstruction_input.bag_adjacency in production.

    Returns a list of dicts matching the row format returned by
    computation_db.connection.get_dict().
    """
    from shapely import Polygon, STRtree

    scale = transform["scale"]
    translate = transform["translate"]
    polygons: list[Polygon] = []
    ids: list[str] = []

    for building_dir in sorted(objects_dir.iterdir()):
        if not building_dir.is_dir():
            continue
        pand_id = building_dir.name
        jsonl_path = building_dir / "reconstruct" / f"{pand_id}.city.jsonl"
        if not jsonl_path.exists():
            continue
        feature = json.loads(jsonl_path.read_text())
        building_obj = feature["CityObjects"].get(pand_id)
        if building_obj is None:
            continue
        lod0_geom = next(
            (g for g in building_obj.get("geometry", []) if g.get("lod") == "0"),
            None,
        )
        if lod0_geom is None:
            continue
        vertices = feature["vertices"]
        boundary = lod0_geom["boundaries"][0][0]  # outer ring of first surface
        coords = [
            (
                vertices[i][0] * scale[0] + translate[0],
                vertices[i][1] * scale[1] + translate[1],
            )
            for i in boundary
        ]
        if len(coords) >= 3:
            polygons.append(Polygon(coords))
            ids.append(pand_id)

    tree = STRtree(polygons)
    rows: list[dict[str, str]] = []
    for i, poly in enumerate(polygons):
        buffered = poly.buffer(0.1)
        for j in tree.query(buffered):
            if i != j and buffered.intersects(polygons[j]):
                rows.append(
                    {
                        "identificatie": ids[i],
                        "adjacent_identificatie": ids[j],
                    }
                )
    return rows


# Computed once at module level so collection is fast on repeated runs.
_ADJACENCY_ROWS = _build_adjacency_from_features(_OBJECTS_DIR, _NL_TRANSFORM)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def integration_file_store(tmp_path: Path) -> FileStoreResource:
    """FileStoreResource with test reconstruction data symlinked into stages/reconstruction/."""
    reconstruction_dir = tmp_path / "stages" / "reconstruction"
    reconstruction_dir.mkdir(parents=True)
    # Symlink z-level dir so the full z/x/y/objects structure is accessible
    (reconstruction_dir / "10").symlink_to(_CROP_RECONSTRUCT / "10")
    return FileStoreResource(root_dir=str(tmp_path))


@pytest.fixture
def adjacency_db() -> MagicMock:
    """Mock DatabaseResource returning adjacency computed from LoD 0 footprints."""
    mock_db = MagicMock()
    mock_db.connection.get_dict.return_value = _ADJACENCY_ROWS
    return mock_db


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------


@pytest.mark.skip("needs local test data")
def test_building_surfaces_integration(
    integration_file_store: FileStoreResource,
    adjacency_db: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run building_surfaces with real data and real shared_walls() for profiling.

    Verifies that the asset processes all buildings and emits a profiling summary.
    The profile JSON is printed to stdout for inspection.
    """
    import bag3d.party_walls.assets.party_walls as _pw_mod
    from bag3d.party_walls.assets.party_walls import (
        PartyWallsConfig,
        building_surfaces,
        features_file_index_generator,
    )

    # Patch real functions into the asset module for the duration of this test.
    # Using monkeypatch (rather than direct assignment) ensures they are restored
    # afterward so the stub-relying unit tests are unaffected.
    monkeypatch.setattr(_pw_mod, "shared_walls", _real_shared_walls)
    monkeypatch.setattr(_pw_mod, "write_cityjsonfeature", _real_write_cityjsonfeature)

    reconstruction_dir = integration_file_store.stage_dir("reconstruction")
    index = dict(features_file_index_generator(reconstruction_dir, max_workers=4))
    assert len(index) > 0, f"No features found in {reconstruction_dir}"

    config = PartyWallsConfig(concurrency=4, profile=True)

    with build_asset_context_for(building_surfaces) as context:
        result = building_surfaces(
            context,
            config,
            index,
            adjacency_db,
            integration_file_store,
            nl_transform,
        )

    assert len(cast(list[Path], result)) > 0

    profile_path = (
        integration_file_store.stage_dir("party_walls")
        / "_profiling"
        / "building_surfaces_profile.json"
    )
    assert profile_path.exists()
    summary = json.loads(profile_path.read_text())
    assert summary["buildings_profiled"] > 0
    print(f"\nProfile summary:\n{json.dumps(summary, indent=2)}")
