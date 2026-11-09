"""Validate serialized field sets, physical types and semantic values."""

import json
import math
from collections.abc import Generator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.specs.semantic import ArrayItemDefinition, Attribute, BaseType


class AttributeValidationOutcome(Enum):
    """Types of outcomes that can happen during attribute validation.

    Possible outcomes and corresponding error codes:

    - NO_ERROR (0): The attribute complies with the specs.
    - BUILDING_EXTRA_ATTRIBUTES (1): The building object has attributes that is should not have.
    - BUILDING_MISSING_ATTRIBUTES (2): The building object is some attributes that are prescribed by the specs.
    - SURFACE_EXTRA_ATTRIBUTES (3): The surface object has attributes that is should not have.
    - SURFACE_MISSING_ATTRIBUTES (4): The surface object is some attributes that are prescribed by the specs.
    - INCORRECT_DATA_TYPE (5): The data type of the attribute does not match the specs.
    - INCORRECT_NULLABLE (6): The nullability of the attribute does not match the specs.
    """

    OK = 0
    BUILDING_EXTRA_ATTRIBUTES = 1
    BUILDING_MISSING_ATTRIBUTES = 2
    SURFACE_EXTRA_ATTRIBUTES = 3
    SURFACE_MISSING_ATTRIBUTES = 4
    INCORRECT_DATA_TYPE = 5
    INCORRECT_NULLABLE = 6
    INCORRECT_VALUE = 7
    MISSING_CONTAINER = 8
    UNEXPECTED_CONTAINER = 9

    @classmethod
    def is_error(cls, outcome: "AttributeValidationOutcome") -> bool:
        """Whether the AttributeValidationError represents an error or not."""
        return outcome != cls.OK


@dataclass(frozen=True)
class AttributeValidationResultOne:
    """The result of the attribute validation for one attribute.
    Includes the attribute name and the error.

    Attributes:
        attribute_name (str): The name of the attribute.
        outcome (AttributeValidationOutcome): The validation outcome.
    """

    attribute_name: str
    outcome: AttributeValidationOutcome


@dataclass
class AttributeValidationResults:
    """The aggregated result of attribute validations for many attributes.

    Attributes:
        results (dict[str, set[AttributeValidationResultOne]]): Map of attribute name, validation results for the attribute.
    """

    results: dict[str, set[AttributeValidationResultOne]] = field(default_factory=dict)

    def all_ok(self) -> bool:
        """Are there any errors in the results?"""
        return len(self.results) == 0

    def add_error(self, result: AttributeValidationResultOne) -> None:
        """Add a single validation result, only if the result is an error."""
        if AttributeValidationOutcome.is_error(result.outcome):
            # The attribute name can be a comma-separated list of attribute names in
            # case of many missing attributes
            for a_name in result.attribute_name.split(","):
                if validation_res := self.results.get(a_name):
                    validation_res.add(result)
                else:
                    self.results[a_name] = {result}

    def __repr__(self):
        def get_value(x: AttributeValidationResultOne) -> int:
            return x.outcome.value

        return f"{ {k: list(map(get_value, v)) for k, v in self.results.items()} }"


def _field_sets(
    expected: set[str], observed: set[str], *, surface: bool = False
) -> Generator[AttributeValidationResultOne, None, None]:
    if extra := observed - expected:
        yield AttributeValidationResultOne(
            ",".join(sorted(extra)),
            AttributeValidationOutcome.SURFACE_EXTRA_ATTRIBUTES
            if surface
            else AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES,
        )
    if missing := expected - observed:
        yield AttributeValidationResultOne(
            ",".join(sorted(missing)),
            AttributeValidationOutcome.SURFACE_MISSING_ATTRIBUTES
            if surface
            else AttributeValidationOutcome.BUILDING_MISSING_ATTRIBUTES,
        )


def _semantic_value(
    value: object, definition: ArrayItemDefinition
) -> AttributeValidationOutcome:
    if value is None:
        return (
            AttributeValidationOutcome.OK
            if definition.nullable
            else AttributeValidationOutcome.INCORRECT_NULLABLE
        )
    kind = definition.type.base_type
    valid_type = {
        BaseType.INT: type(value) is int,
        BaseType.FLOAT: type(value) in (int, float),
        BaseType.BOOL: type(value) is bool,
        BaseType.STRING: isinstance(value, str),
        BaseType.DATE: isinstance(value, str),
        BaseType.DATETIME: isinstance(value, str),
        BaseType.ARRAY: isinstance(value, list),
    }[kind]
    if not valid_type:
        return AttributeValidationOutcome.INCORRECT_DATA_TYPE
    if kind == BaseType.FLOAT and not math.isfinite(value):
        return AttributeValidationOutcome.INCORRECT_VALUE
    if definition.values is not None:
        key = str(value).lower() if isinstance(value, bool) else str(value)
        if key not in definition.values:
            return AttributeValidationOutcome.INCORRECT_VALUE
    if kind in (BaseType.DATE, BaseType.DATETIME):
        # Check the logical calendar type, never the descriptive valueFormat.
        try:
            if kind == BaseType.DATE:
                date.fromisoformat(value)
            else:
                datetime.fromisoformat(value)
        except ValueError:
            return AttributeValidationOutcome.INCORRECT_VALUE
    if kind == BaseType.ARRAY and isinstance(definition, Attribute):
        for item in value:
            outcome = _semantic_value(item, definition.items)
            if outcome != AttributeValidationOutcome.OK:
                return outcome
    return AttributeValidationOutcome.OK


def validate_value(
    value: object, definition: Attribute, encoding: str | None
) -> AttributeValidationOutcome:
    if value is not None and encoding == "json":
        if not isinstance(value, str):
            return AttributeValidationOutcome.INCORRECT_DATA_TYPE
        try:
            value = json.loads(value)
        except ValueError:
            return AttributeValidationOutcome.INCORRECT_VALUE
    if value is not None and encoding == "boolean-integer":
        if type(value) is not int or value not in (0, 1):
            return AttributeValidationOutcome.INCORRECT_VALUE
        value = bool(value)
    return _semantic_value(value, definition)


def _cityjson_values(
    specs: Specs3DBAGResource,
    observed: dict,
    projected: Mapping,
    *,
    surface: bool = False,
) -> Generator[AttributeValidationResultOne, None, None]:
    names = {entry.property: name for name, entry in projected.items()}
    yield from _field_sets(set(names), set(observed), surface=surface)
    for physical_name in names.keys() & observed.keys():
        name = names[physical_name]
        entry = projected[name]
        value = observed[physical_name]
        physical_ok = (
            value is None
            or {
                "string": isinstance(value, str),
                "number": type(value) in (int, float),
                "boolean": type(value) is bool,
                "array": isinstance(value, list),
                "object": isinstance(value, dict),
            }[entry.type]
        )
        outcome = (
            validate_value(value, specs.attributes[name], entry.encoding)
            if physical_ok
            else AttributeValidationOutcome.INCORRECT_DATA_TYPE
        )
        if outcome != AttributeValidationOutcome.OK:
            yield AttributeValidationResultOne(physical_name, outcome)


def cityobject_validate_attributes(
    specs: Specs3DBAGResource, co: dict
) -> Generator[AttributeValidationResultOne, None, None]:
    object_type = co["type"]
    if object_type not in specs.cityjson.objects:
        yield AttributeValidationResultOne(
            object_type, AttributeValidationOutcome.UNEXPECTED_CONTAINER
        )
    else:
        yield from _cityjson_values(
            specs, co.get("attributes", {}), specs.cityjson_attributes(object_type)
        )
    for geometry in co.get("geometry", []):
        for surface in geometry.get("semantics", {}).get("surfaces", []):
            surface_type = surface["type"]
            if surface_type not in specs.cityjson.semantic_surfaces:
                yield AttributeValidationResultOne(
                    surface_type, AttributeValidationOutcome.UNEXPECTED_CONTAINER
                )
                continue
            observed = {
                key: value
                for key, value in surface.items()
                if key not in {"type", "parent", "children"}
            }
            yield from _cityjson_values(
                specs,
                observed,
                specs.cityjson_attributes(surface_type, surface=True),
                surface=True,
            )


def gpkg_validate_attributes(
    specs: Specs3DBAGResource, gpkg_info: dict
) -> Generator[AttributeValidationResultOne, None, None]:
    observed = {layer["name"]: layer for layer in gpkg_info["layers"]}
    expected = {container.name: container for container in specs.geopackage.containers}
    for name in expected.keys() - observed.keys():
        yield AttributeValidationResultOne(
            name, AttributeValidationOutcome.MISSING_CONTAINER
        )
    for name in observed.keys() - expected.keys():
        if not name.startswith(("gpkg_", "rtree_", "sqlite_")):
            yield AttributeValidationResultOne(
                name, AttributeValidationOutcome.UNEXPECTED_CONTAINER
            )
    ogr_types = {
        "TEXT": {"String"},
        "REAL": {"Real"},
        "FLOAT": {"Real"},
        "INTEGER": {"Integer", "Integer64"},
        "BOOLEAN": {"Integer(Boolean)"},
        "DATE": {"Date"},
        "DATETIME": {"DateTime"},
    }
    for name in expected.keys() & observed.keys():
        container = expected[name]
        layer = observed[name]
        fields = {entry["name"]: entry for entry in layer["fields"]}
        projected = {
            entry.column: attr_name for attr_name, entry in container.attributes.items()
        }
        structural = set(container.structural_columns)
        yield from _field_sets(
            set(projected),
            fields.keys() - structural,
            surface=container.kind == "semantics",
        )
        for column in projected.keys() & fields.keys():
            attr_name = projected[column]
            entry = container.attributes[attr_name]
            observed_field = fields[column]
            # OGR reports BOOLEAN as Integer + Boolean subtype in JSON output.
            observed_type = observed_field["type"]
            if observed_field.get("subType") == "Boolean":
                observed_type = "Integer(Boolean)"
            if observed_type not in ogr_types[entry.type]:
                yield AttributeValidationResultOne(
                    f"{name}.{column}", AttributeValidationOutcome.INCORRECT_DATA_TYPE
                )
            if (
                "storageType" in observed_field
                and observed_field["storageType"].upper() != entry.type
            ):
                yield AttributeValidationResultOne(
                    f"{name}.{column}", AttributeValidationOutcome.INCORRECT_DATA_TYPE
                )
            if observed_field.get("nullable") != specs.attributes[attr_name].nullable:
                yield AttributeValidationResultOne(
                    f"{name}.{column}", AttributeValidationOutcome.INCORRECT_NULLABLE
                )
