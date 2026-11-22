"""Write one enriched reconstruction tile from Roofer and val3dity pipes."""

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from subprocess import PIPE, CalledProcessError, Popen
from typing import BinaryIO
from uuid import uuid4

LOD_FIELDS = {
    "1.2": "b3_val3dity_lod12",
    "1.3": "b3_val3dity_lod13",
    "2.2": "b3_val3dity_lod22",
}
UNSUITABLE_REASONS = {"_HIGHEST_YET_INSUFFICIENT_COVERAGE", "_LATEST_BUT_OUTDATED"}


@dataclass
class TileCounts:
    buildings: int = 0
    buildings_with_errors: int = 0


def _encode(record: dict) -> bytes:
    return (
        json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        + b"\n"
    )


def _lod_feature(feature: dict, lod: str) -> dict | None:
    """Keep the building and its parts together so val3dity can check the whole LoD."""
    fid = feature["id"]
    objects = feature["CityObjects"]
    building = objects[fid]
    projected = {}
    children = []
    for oid in (fid, *building.get("children", [])):
        obj = objects[oid]
        geometries = [g for g in obj.get("geometry", []) if str(g["lod"]) == lod]
        if oid == fid or geometries:
            projected[oid] = {"type": obj["type"], "geometry": geometries}
            if oid != fid:
                projected[oid]["parents"] = [fid]
                children.append(oid)
    if not children and not projected[fid]["geometry"]:
        return None
    if children:
        projected[fid]["children"] = children
    return {
        "type": "CityJSONFeature",
        "id": fid,
        "CityObjects": projected,
        "vertices": feature["vertices"],
    }


def enrich_building(feature: dict, validate: Callable[[bytes, str], list[int]]) -> bool:
    """Set published attributes and return whether any validated LoD reported errors."""
    fid = feature["id"]
    building = feature["CityObjects"][fid]
    if feature["type"] != "CityJSONFeature" or building["type"] != "Building":
        raise ValueError(f"Expected a Roofer building feature, got {fid!r}")

    attrs = building.setdefault("attributes", {})
    has_errors = False
    for lod, field in LOD_FIELDS.items():
        view = _lod_feature(feature, lod)
        if view is None:
            attrs[field] = None
        else:
            codes = validate(_encode(view), fid)
            attrs[field] = json.dumps(sorted(set(codes)), separators=(",", ":"))
            has_errors |= bool(codes)

    reason = attrs.get("b3_pw_selectie_reden")
    attrs["b3_kwaliteitsindicator"] = (
        attrs.get("b3_bag_bag_overlap") == 0
        and attrs["b3_val3dity_lod22"] == "[]"
        and isinstance(reason, str)
        and reason not in UNSUITABLE_REASONS
    )
    return has_errors


def _request(
    stdin: BinaryIO, stdout: BinaryIO, record: bytes, expected: str
) -> list[int]:
    stdin.write(record)
    stdin.flush()
    response = stdout.readline()
    if not response:
        raise RuntimeError(f"val3dity closed stdout before replying to {expected!r}")
    # The protocol returns two JSON values on one line, a quoted ID and an array.
    line = response.decode("utf-8")
    try:
        reply_id, end = json.JSONDecoder().raw_decode(line)
        codes = json.loads(line[end:])
    except ValueError as exc:
        raise ValueError(
            f"Malformed val3dity reply for {expected!r}: {line[:200]!r}"
        ) from exc
    if (
        reply_id != expected
        or not isinstance(codes, list)
        or any(type(c) is not int for c in codes)
    ):
        raise ValueError(f"Unexpected val3dity reply for {expected!r}: {line[:200]!r}")
    return codes


@contextmanager
def _child(args: list[str], **kwargs) -> Iterator[Popen[bytes]]:
    # Inherit stderr so Dagster compute logs receive both tools' diagnostics live.
    with Popen(args, stdout=PIPE, **kwargs) as child:
        try:
            yield child
        finally:
            if child.poll() is None:
                child.kill()


def write_validated_tile(
    roofer_args: list[str], roofer_env: dict[str, str], val3dity: str, target: Path
) -> TileCounts:
    # Match the tolerances used by export.validate for the published geometries.
    validator_args = [
        val3dity,
        "--planarity_n_tol",
        "20",
        "--planarity_d2p_tol",
        "0.0001",
        "--snap_tol",
        "0.0001",
        "stdin",
    ]
    partial_path = target.with_name(f".{target.name}.{uuid4().hex}.partial")
    counts = TileCounts()
    try:
        with (
            partial_path.open("xb") as output,
            _child(roofer_args, env=roofer_env) as roofer,
            _child(validator_args, stdin=PIPE) as validator,
        ):
            assert roofer.stdout and validator.stdin and validator.stdout

            validate = partial(_request, validator.stdin, validator.stdout)

            header = roofer.stdout.readline()
            if not header or json.loads(header).get("type") != "CityJSON":
                raise ValueError("Expected a CityJSONSeq header from Roofer")
            header = header.rstrip(b"\r\n") + b"\n"
            if validate(header, "1st-line"):
                raise ValueError("val3dity rejected the CityJSONSeq header")
            output.write(header)
            for line in roofer.stdout:
                feature = json.loads(line)
                if enrich_building(feature, validate):
                    counts.buildings_with_errors += 1
                output.write(_encode(feature))
                counts.buildings += 1

            validator.stdin.close()
            if validator.stdout.readline():
                raise ValueError("Unexpected trailing val3dity output")
            for child in (roofer, validator):
                if child.wait() != 0:
                    raise CalledProcessError(child.returncode, child.args)
        partial_path.replace(target)
    finally:
        partial_path.unlink(missing_ok=True)
    return counts
