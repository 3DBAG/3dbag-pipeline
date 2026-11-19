"""Streetlidar LAS header CRS validation."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest
from bag3d.common.resources.executables import CommandResult, PDALResource
from bag3d.core.assets.streetlidar import inspect_tile


@pytest.mark.parametrize("code", [None, 28992, 7415, 4326, 4979])
def test_header_crs_validation(code):
    from pyproj import CRS as ProjCRS

    metadata: dict[str, int | str] = dict(minx=1, miny=2, maxx=3, maxy=4, count=20)
    if code:
        metadata["spatialreference"] = ProjCRS.from_epsg(code).to_wkt()
    runner = SimpleNamespace(
        with_docker=False,
        run=lambda *args, **kwargs: CommandResult(
            0, json.dumps({"metadata": metadata})
        ),
    )
    pdal = create_autospec(PDALResource, instance=True, runner=runner)
    if code in {4326, 4979}:
        with pytest.raises(ValueError, match="horizontal CRS"):
            inspect_tile(Path("tile.laz"), pdal)
    else:
        bounds, count, _ = inspect_tile(Path("tile.laz"), pdal)
        assert bounds == (1, 2, 3, 4) and count == 20


def test_conflicting_vertical_crs():
    from pyproj.crs import CompoundCRS

    wkt = CompoundCRS("RD with wrong heights", components=[28992, 5773]).to_wkt()
    metadata: dict[str, int | str] = dict(
        minx=1, miny=2, maxx=3, maxy=4, count=20, spatialreference=wkt
    )
    runner = SimpleNamespace(
        with_docker=False,
        run=lambda *args, **kwargs: CommandResult(
            0, json.dumps({"metadata": metadata})
        ),
    )
    with pytest.raises(ValueError, match="vertical CRS"):
        inspect_tile(
            Path("tile.laz"),
            create_autospec(PDALResource, instance=True, runner=runner),
        )
