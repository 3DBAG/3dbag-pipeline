"""Inspect serialized tilesets and their glTF structural metadata."""

import itertools
import json
import struct
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import unquote, urlsplit

import meshopt
from bag3d.common.resources.specs import Specs3DBAGResource

from bag3d.export.assets.export.attributes import (
    AttributeValidationOutcome as Outcome,
)
from bag3d.export.assets.export.attributes import (
    AttributeValidationResultOne as Result,
)
from bag3d.export.assets.export.attributes import (
    validate_value,
)


def _local_uri(base: Path, uri: str) -> Path:
    parsed = urlsplit(uri)
    if parsed.scheme or parsed.netloc or "{" in uri:
        raise ValueError(f"Unsupported tileset URI {uri!r} in {base}")
    return base / unquote(parsed.path)


def _content_paths(path: Path, visited: set[Path]) -> Iterator[Path]:
    if path in visited:
        raise ValueError(f"Cyclic tileset reference: {path}")
    visited.add(path)
    tileset = json.loads(path.read_text(encoding="utf-8"))
    stack = [tileset["root"]]
    while stack:
        tile = stack.pop()
        if "implicitTiling" in tile:
            raise ValueError(f"Unsupported implicit tiling in {path}")
        stack.extend(tile.get("children", []))
        contents = tile.get("contents", []) + (
            [tile["content"]] if "content" in tile else []
        )
        for content in contents:
            target = _local_uri(path.parent, content["uri"])
            if target.suffix == ".json":
                yield from _content_paths(target, visited)
            else:
                yield target
    visited.remove(path)


def _read_gltf(path: Path) -> tuple[dict, list[bytes]]:
    binary = None
    if path.suffix.lower() == ".glb":
        data = path.read_bytes()
        if len(data) < 20 or data[:4] != b"glTF":
            raise ValueError(f"Invalid GLB header: {path}")
        _, version, size = struct.unpack_from("<4sII", data)
        if version != 2 or size != len(data):
            raise ValueError(f"Invalid GLB version or length: {path}")
        chunks = {}
        offset = 12
        while offset < size:
            length, kind = struct.unpack_from("<II", data, offset)
            if offset + 8 + length > size:
                raise ValueError(f"Truncated GLB chunk: {path}")
            chunks[kind] = data[offset + 8 : offset + 8 + length]
            offset += 8 + length
        model = json.loads(chunks[0x4E4F534A])
        binary = chunks.get(0x004E4942)
    elif path.suffix.lower() == ".gltf":
        model = json.loads(path.read_text(encoding="utf-8"))
    else:
        raise ValueError(f"Unsupported 3D Tiles content: {path}")
    buffers = []
    for index, entry in enumerate(model.get("buffers", [])):
        if "uri" in entry:
            buffers.append(_local_uri(path.parent, entry["uri"]).read_bytes())
        elif index == 0 and binary is not None:
            buffers.append(binary)
        else:
            # Meshopt's fallback geometry buffer may have no bytes; only fail if used.
            buffers.append(b"")
    return model, buffers


def _view(model: dict, buffers: list[bytes], index: int) -> bytes:
    view = model["bufferViews"][index]
    compressed = view.get("extensions", {}).get("EXT_meshopt_compression")
    if compressed is not None:
        buffer = buffers[compressed["buffer"]]
        start = compressed.get("byteOffset", 0)
        end = start + compressed["byteLength"]
        if end > len(buffer):
            raise ValueError(
                f"Compressed metadata bufferView {index} exceeds its buffer"
            )
        decoded = meshopt.decode_gltf_buffer(
            buffer[start:end],
            compressed["count"],
            compressed["byteStride"],
            compressed["mode"],
            compressed.get("filter", "NONE"),
        ).tobytes()
        if len(decoded) != view["byteLength"]:
            raise ValueError(
                f"Metadata bufferView {index} has an incorrect decoded length"
            )
        return decoded
    buffer = buffers[view["buffer"]]
    start = view.get("byteOffset", 0)
    end = start + view["byteLength"]
    if end > len(buffer):
        raise ValueError(f"Metadata bufferView {index} exceeds the serialized buffer")
    return buffer[start:end]


def _scalars(data: bytes, component_type: str, count: int) -> list:
    formats = {
        "INT8": "b",
        "UINT8": "B",
        "INT16": "h",
        "UINT16": "H",
        "INT32": "i",
        "UINT32": "I",
        "INT64": "q",
        "UINT64": "Q",
        "FLOAT32": "f",
        "FLOAT64": "d",
    }
    if component_type not in formats:
        raise ValueError(f"Unsupported metadata component {component_type}")
    code = formats[component_type]
    length = count * struct.calcsize(code)
    if len(data) < length:
        raise ValueError("Truncated metadata values")
    return list(struct.unpack_from(f"<{count}{code}", data))


def _values(
    model: dict, buffers: list[bytes], prop: dict, table_prop: dict, count: int
) -> list:
    if prop.get("array", False):
        raise ValueError(
            "Native metadata array decoding is unsupported by this distribution"
        )
    data = _view(model, buffers, table_prop["values"])
    if prop["type"] == "STRING":
        offsets = _scalars(
            _view(model, buffers, table_prop["stringOffsets"]),
            table_prop.get("stringOffsetType", "UINT32"),
            count + 1,
        )
        if (
            offsets[0] != 0
            or offsets[-1] > len(data)
            or any(a > b for a, b in itertools.pairwise(offsets))
        ):
            raise ValueError("Invalid metadata string offsets")
        return [data[offsets[i] : offsets[i + 1]].decode("utf-8") for i in range(count)]
    if prop["type"] == "BOOLEAN":
        if len(data) * 8 < count:
            raise ValueError("Truncated boolean metadata")
        return [bool(data[i // 8] & (1 << (i % 8))) for i in range(count)]
    return _scalars(data, prop["componentType"], count)


def inspect_tileset(
    directory: Path, lod: str, specs: Specs3DBAGResource
) -> Iterator[Result]:
    projected = specs.tileset(lod)
    found_content = False
    for path in _content_paths(directory / "tileset.json", set()):
        found_content = True
        model, buffers = _read_gltf(path)
        metadata = model.get("extensions", {}).get("EXT_structural_metadata")
        if metadata is None:
            yield Result(str(path), Outcome.MISSING_CONTAINER)
            continue
        schema = metadata.get("schema")
        if schema is None and "schemaUri" in metadata:
            schema = json.loads(
                _local_uri(path.parent, metadata["schemaUri"]).read_text(
                    encoding="utf-8"
                )
            )
        if schema is None:
            raise ValueError(f"No metadata schema in {path}")
        classes = schema["classes"]
        if projected.class_name not in classes:
            yield Result(f"{path}:{projected.class_name}", Outcome.MISSING_CONTAINER)
        for name in classes.keys() - {projected.class_name}:
            yield Result(f"{path}:{name}", Outcome.UNEXPECTED_CONTAINER)
        tables = metadata.get("propertyTables", [])
        if not tables:
            yield Result(f"{path}:propertyTables", Outcome.MISSING_CONTAINER)
        for table in tables:
            class_name = table["class"]
            if class_name != projected.class_name:
                yield Result(f"{path}:{class_name}", Outcome.UNEXPECTED_CONTAINER)
                continue
            if class_name not in classes:
                continue
            properties = classes[class_name]["properties"]
            expected = {
                entry.property: (name, entry)
                for name, entry in projected.properties.items()
            }
            for source in (properties, table["properties"]):
                for name in expected.keys() - source.keys():
                    yield Result(f"{path}:{name}", Outcome.BUILDING_MISSING_ATTRIBUTES)
                for name in source.keys() - expected.keys():
                    yield Result(f"{path}:{name}", Outcome.BUILDING_EXTRA_ATTRIBUTES)
            for name in (
                expected.keys() & properties.keys() & table["properties"].keys()
            ):
                semantic_name, entry = expected[name]
                prop = properties[name]
                component_type = prop.get("componentType", prop["type"])
                shape = "array" if prop.get("array", False) else "scalar"
                if (
                    component_type != entry.component_type
                    or shape != entry.shape
                    or prop["type"]
                    != (
                        entry.component_type
                        if entry.component_type in {"STRING", "BOOLEAN"}
                        else "SCALAR"
                    )
                ):
                    yield Result(f"{path}:{name}", Outcome.INCORRECT_DATA_TYPE)
                    continue
                if prop.get("noData") != entry.no_data or "default" in prop:
                    yield Result(f"{path}:{name}", Outcome.INCORRECT_NULLABLE)
                if prop.get("normalized", False) or "offset" in prop or "scale" in prop:
                    yield Result(f"{path}:{name}", Outcome.INCORRECT_VALUE)
                for value in _values(
                    model, buffers, prop, table["properties"][name], table["count"]
                ):
                    if value == entry.no_data or (
                        entry.null_encoding == "empty-string" and value == ""
                    ):
                        value = None
                    outcome = validate_value(
                        value, specs.attributes[semantic_name], entry.encoding
                    )
                    if outcome != Outcome.OK:
                        yield Result(f"{path}:{name}", outcome)
                        break
    if not found_content:
        yield Result(
            f"{directory}:no serialized content for {lod}", Outcome.MISSING_CONTAINER
        )
