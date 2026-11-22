import json
import os
import sys
from copy import deepcopy
from subprocess import CalledProcessError

import pytest

from bag3d.core.assets.reconstruction.stream import (
    enrich_building,
    write_validated_tile,
)


def building():
    return {
        "type": "CityJSONFeature",
        "id": "building",
        "vertices": [[1, 2, 3]],
        "CityObjects": {
            "building": {
                "type": "Building",
                "children": ["part-A", "part-B"],
                "geometry": [{"lod": "0", "type": "MultiSurface"}],
                "attributes": {
                    "b3_bag_bag_overlap": 0,
                    "b3_pw_selectie_reden": "PREFERRED_AND_LATEST",
                },
            },
            "part-A": {
                "type": "BuildingPart",
                "geometry": [{"lod": "1.2"}, {"lod": "2.2"}],
            },
            "part-B": {"type": "BuildingPart", "geometry": [{"lod": "2.2"}]},
        },
    }


@pytest.mark.parametrize(
    ("overlap", "reason", "errors22", "quality"),
    [
        (0, "PREFERRED_AND_LATEST", [], True),
        (0, "_HIGHEST_YET_INSUFFICIENT_COVERAGE", [], False),
        (0, "_LATEST_BUT_OUTDATED", [], False),
        (0.01, "PREFERRED_AND_LATEST", [], False),
        (0, "PREFERRED_AND_LATEST", [302], False),
        (None, "PREFERRED_AND_LATEST", [], False),
        (0, None, [], False),
    ],
)
def test_enrich_building(overlap, reason, errors22, quality):
    feature = building()
    attrs = feature["CityObjects"]["building"]["attributes"]
    attrs.update(b3_bag_bag_overlap=overlap, b3_pw_selectie_reden=reason)
    original = deepcopy(feature)
    calls = []

    def validate(line, fid):
        view = json.loads(line)
        assert fid == view["id"] == "building"
        assert view["vertices"] == feature["vertices"]
        objects = view["CityObjects"]
        children = objects[fid]["children"]
        (lod,) = {g["lod"] for obj in objects.values() for g in obj["geometry"]}
        assert children == (["part-A"] if lod == "1.2" else ["part-A", "part-B"])
        calls.append(lod)
        return [302, 101, 302] if lod == "1.2" else errors22

    assert enrich_building(feature, validate) is True  # LoD1.2 has errors.
    assert calls == ["1.2", "2.2"]
    assert attrs["b3_val3dity_lod12"] == "[101,302]"
    assert attrs["b3_val3dity_lod13"] is None
    assert attrs["b3_val3dity_lod22"] == json.dumps(errors22)
    assert attrs["b3_kwaliteitsindicator"] is quality
    assert {
        k: v
        for k, v in attrs.items()
        if not k.startswith(("b3_val3dity_", "b3_kwaliteit"))
    } == original["CityObjects"]["building"]["attributes"]
    feature["CityObjects"]["building"]["attributes"] = original["CityObjects"][
        "building"
    ]["attributes"]
    assert feature == original


def test_absent_lod22_is_not_valid():
    feature = building()
    feature["CityObjects"]["building"]["children"] = []
    enrich_building(feature, lambda *_: pytest.fail("no 3D LoD to validate"))
    attrs = feature["CityObjects"]["building"]["attributes"]
    assert attrs["b3_val3dity_lod22"] is None
    assert attrs["b3_kwaliteitsindicator"] is False


@pytest.mark.parametrize(
    "failure",
    [
        "wrong-id",
        "malformed",
        "early-eof",
        "extra-reply",
        "validator-exit",
        "roofer-exit",
        "",
    ],
)
def test_tile_is_published_only_on_complete_stream(tmp_path, failure):
    header = {
        "type": "CityJSON",
        "version": "2.0",
        "CityObjects": {},
        "vertices": [],
        "transform": {"scale": [1, 1, 1], "translate": [0, 0, 0]},
    }
    source = "\n".join(map(json.dumps, (header, building())))
    roofer = [
        sys.executable,
        "-c",
        f"print({source!r}); raise SystemExit({int(failure == 'roofer-exit')})",
    ]
    validator = tmp_path / "validator"
    validator.write_text(
        f"#!{sys.executable}\n"
        "import sys, json\n"
        f"failure = {failure!r}\n"
        "for line in sys.stdin:\n"
        "    record = json.loads(line)\n"
        "    if record['type'] == 'CityJSON': print('\"1st-line\" []', flush=True); continue\n"
        "    if failure == 'early-eof': sys.exit(1)\n"
        "    if failure == 'malformed': print('invalid', flush=True); continue\n"
        "    fid = 'wrong' if failure == 'wrong-id' else record['id']\n"
        "    print(json.dumps(fid), '[]', flush=True)\n"
        "if failure == 'extra-reply': print('extra')\n"
        "sys.exit(int(failure == 'validator-exit'))\n"
    )
    validator.chmod(0o755)
    target = tmp_path / "716.city.jsonl"
    target.write_bytes(b"previous generation")
    if failure:
        with pytest.raises((CalledProcessError, ValueError, RuntimeError)):
            write_validated_tile(roofer, os.environ.copy(), str(validator), target)
        assert target.read_bytes() == b"previous generation"
    else:
        counts = write_validated_tile(roofer, os.environ.copy(), str(validator), target)
        assert counts.buildings == 1
        assert counts.buildings_with_errors == 0
        root, feature = map(json.loads, target.read_bytes().splitlines())
        assert root == header
        assert (
            feature["CityObjects"]["building"]["attributes"]["b3_kwaliteitsindicator"]
            is True
        )
    assert not list(tmp_path.glob("*.partial"))
