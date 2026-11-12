"""Tests for the IFC export asset and conversion logic."""

import json
import zipfile
from typing import cast
from unittest.mock import patch

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource

from bag3d.export.assets.export.archive import compress_files
from bag3d.export.assets.export.ifc import IFCConfig, reconstruction_output_ifc
from bag3d.export.ifc.convert import convert_cityjson_to_ifc, load_cityjson

VERSION = "test_version"


def _minimal_cityjson() -> dict:
    """A minimal 3DBAG-style CityJSON with a Building and a BuildingPart."""
    return {
        "type": "CityJSON",
        "version": "2.0",
        "transform": {
            "scale": [0.001, 0.001, 0.001],
            "translate": [85000.0, 446000.0, 0.0],
        },
        "metadata": {"referenceSystem": "https://www.opengis.net/def/crs/EPSG/0/7415"},
        "vertices": [
            [0, 0, 0],
            [10000, 0, 0],
            [10000, 10000, 0],
            [0, 10000, 0],
            [0, 0, 5000],
            [10000, 0, 5000],
            [10000, 10000, 5000],
            [0, 10000, 5000],
        ],
        "CityObjects": {
            "NL.IMBAG.Pand.0503100000033172": {
                "type": "Building",
                "attributes": {
                    "identificatie": "NL.IMBAG.Pand.0503100000033172",
                    "b3_bouwlagen": 3,
                    "oorspronkelijkbouwjaar": 1990,
                },
                "children": ["NL.IMBAG.Pand.0503100000033172-0"],
                "geometry": [
                    {"type": "MultiSurface", "lod": "0", "boundaries": [[[0, 1, 2, 3]]]}
                ],
            },
            "NL.IMBAG.Pand.0503100000033172-0": {
                "type": "BuildingPart",
                "parents": ["NL.IMBAG.Pand.0503100000033172"],
                "geometry": [
                    {
                        "type": "MultiSurface",
                        "lod": "1.2",
                        "boundaries": [[[0, 1, 2, 3]]],
                    }
                ],
            },
        },
    }


def test_load_cityjson(tmp_path):
    """load_cityjson parses the object model and frees the raw CityObjects."""
    path = tmp_path / "tile.city.json"
    path.write_text(json.dumps(_minimal_cityjson()))

    with path.open("r") as infile:
        cm = load_cityjson(infile)

    cityobjects = cast(dict, cm.get_cityobjects())
    transform = cast(dict, cm.transform)
    assert "NL.IMBAG.Pand.0503100000033172" in cityobjects
    assert "NL.IMBAG.Pand.0503100000033172-0" in cityobjects
    assert transform["scale"] == [0.001, 0.001, 0.001]
    assert transform["translate"] == [85000.0, 446000.0, 0.0]
    assert cm.get_epsg() == 7415
    # The raw CityObjects JSON member is freed after loading
    assert cm.j["CityObjects"] == {}


def test_convert_cityjson_to_ifc(tmp_path):
    """convert_cityjson_to_ifc writes one IFC file per LoD."""
    path = tmp_path / "tile.city.json"
    path.write_text(json.dumps(_minimal_cityjson()))

    files = convert_cityjson_to_ifc(path)

    assert {f.name for f in files} == {
        "tile-0.ifc",
        "tile-1.2.ifc",
        "tile-1.3.ifc",
        "tile-2.2.ifc",
    }
    for f in files:
        assert f.exists()
        assert f.stat().st_size > 0


def test_convert_cityjson_to_ifc_no_transform(tmp_path):
    """A CityJSON tile without a transform (absolute coordinates) still converts."""
    cj = _minimal_cityjson()
    cj.pop("transform")
    path = tmp_path / "tile.city.json"
    path.write_text(json.dumps(cj))

    files = convert_cityjson_to_ifc(path)

    assert {f.name for f in files} == {
        "tile-0.ifc",
        "tile-1.2.ifc",
        "tile-1.3.ifc",
        "tile-2.2.ifc",
    }
    for f in files:
        assert f.exists()
        assert f.stat().st_size > 0


def test_compress_files_zips_ifc(tmp_path):
    """compress_files zips the per-LoD IFC files and removes the originals."""
    export_dir = tmp_path / "export"
    tile_id = "10/434/716"
    lid = tile_id.replace("/", "-")
    base = export_dir / "t" / tile_id
    base.parent.mkdir(parents=True, exist_ok=True)

    (base.with_suffix(".city.json")).write_text("{}")
    ifc_files = []
    for lod in ("0", "1.2", "1.3", "2.2"):
        ifc_path = base.with_name(f"{base.name}-{lod}.ifc")
        ifc_path.write_text(f"ifc-{lod}")
        ifc_files.append(ifc_path)

    compress_files((tile_id, export_dir))

    ifc_zip = base.parent / f"{lid}-ifc.zip"
    assert ifc_zip.exists()
    with zipfile.ZipFile(ifc_zip) as zf:
        assert sorted(zf.namelist()) == sorted(
            f"{base.name}-{lod}.ifc" for lod in ("0", "1.2", "1.3", "2.2")
        )
    for ifc_path in ifc_files:
        assert not ifc_path.exists()


class _SyncPool:
    """A synchronous stand-in for ProcessPoolExecutor."""

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def map(self, func, iterable):
        return [func(item) for item in iterable]


def test_reconstruction_output_ifc(tmp_path):
    """reconstruction_output_ifc converts all CityJSON tiles to IFC files."""
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version=VERSION)

    export_dir = tmp_path / "stages" / "export" / VERSION
    base = export_dir / "t" / "10/434/716"
    base.parent.mkdir(parents=True, exist_ok=True)
    (base.with_suffix(".city.json")).write_text(json.dumps(_minimal_cityjson()))

    with patch("bag3d.export.assets.export.ifc.ProcessPoolExecutor", _SyncPool):
        reconstruction_output_ifc(IFCConfig(concurrency=1), file_store, version)

    for lod in ("0", "1.2", "1.3", "2.2"):
        assert (base.with_name(f"{base.name}-{lod}.ifc")).exists()
