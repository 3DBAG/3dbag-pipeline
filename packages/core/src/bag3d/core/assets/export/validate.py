from enum import Enum
from pathlib import Path
import json
import re
import csv
import ast
from dataclasses import dataclass, field
from typing import Generator

from dagster import asset, AssetIn, AssetKey, OpExecutionContext, get_dagster_logger

from bag3d.specs.core import CityJSONLocation, GpkgLocation
from bag3d.common.resources.executables import execute_shell_command_silent, AppImage
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.utils.files import bag3d_export_dir

logger = get_dagster_logger("validate")


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

        return f"{dict((k, list(map(get_value, v))) for k, v in self.results.items())}"


@dataclass
class CityJSONFileResults:
    """Results of the compressed CityJSON file validation for a single tile.

    Attributes:
        zip_ok (bool): Whether the file is successfully compressed.
        file_ok (bool): Whether the CityJSON file itself is valid.
        nr_building (int): Number of building features.
        nr_buildingpart (int): Number of building part features.
        nr_invalid_building (int): Number of invalid building features. If any of the
            building part geometries is invalid, the building feature is invalid.
        nr_invalid_buildingpart_lod12 (int): Number of invalid LoD1.2 geometries.
        nr_invalid_buildingpart_lod13 (int): Number of invalid LoD1.3 geometries.
        nr_invalid_buildingpart_lod22 (int): Number of invalid LoD2.2 geometries.
        errors_lod12 (list[int]): List of val3dity error codes of the LoD1.2 geometries.
        errors_lod13 (list[int]): List of val3dity error codes of the LoD1.3 geometries.
        errors_lod22 (list[int]): List of val3dity error codes of the LoD2.2 geometries.
        nr_mismatch_errors_lod12 (int): Number of LoD1.2 building parts that have a
            different set of val3dtiy error codes compared to the `b3_val3dity_lod12`
            attribute.
        nr_mismatch_errors_lod13 (int): Number of LoD1.3 building parts that have a
            different set of val3dtiy error codes compared to the `b3_val3dity_lod13`
            attribute.
        nr_mismatch_errors_lod22 (int): Number of LoD2.2 building parts that have a
            different set of val3dtiy error codes compared to the `b3_val3dity_lod22`
            attribute.
        lod (list[str]): List of LoDs in the CityJSON file.
        schema_valid (bool): Whether or not the schema of the CityJSON is valid.
        schema_warnings (bool): Whether or not the schema of the CityJSON has warnings.
        attributes_with_errors (AttributeValidationResults): List of attribute names with the error codes that they have.
        download (str): The URL of the file download.
        sha256 (str): The SHA256 of the zipfile.
    """

    zip_ok: bool = None
    file_ok: bool = None
    nr_building: int = None
    nr_buildingpart: int = None
    nr_invalid_building: int = None
    nr_invalid_buildingpart_lod12: int = None
    nr_invalid_buildingpart_lod13: int = None
    nr_invalid_buildingpart_lod22: int = None
    errors_lod12: list[int] = None
    errors_lod13: list[int] = None
    errors_lod22: list[int] = None
    nr_mismatch_errors_lod12: int = None
    nr_mismatch_errors_lod13: int = None
    nr_mismatch_errors_lod22: int = None
    lod: list[str] = None
    schema_valid: bool = None
    schema_warnings: bool = None
    attributes_with_errors: AttributeValidationResults = field(
        default_factory=AttributeValidationResults
    )
    download: str = None
    sha256: str = None

    def asdict(self) -> dict:
        return {f"cj_{k}": v for k, v in self.__dict__.items()}


@dataclass
class OBJFileResults:
    """Results of the compressed OBJ files validation for a single tile.

    Attributes:
        zip_ok (bool): Whether the file is successfully compressed.
        file_ok (bool): Whether the OBJ file itself is valid.
        nr_building (int): Number of building features.
        nr_buildingpart (int): Number of building part features.
        nr_invalid_building (int): Number of invalid building features. If any of the
            building part geometries is invalid, the building feature is invalid.
        nr_invalid_buildingpart_lod12 (int): Number of invalid LoD1.2 geometries.
        nr_invalid_buildingpart_lod13 (int): Number of invalid LoD1.3 geometries.
        nr_invalid_buildingpart_lod22 (int): Number of invalid LoD2.2 geometries.
        errors_lod12 (list[int]): List of val3dity error codes of the LoD1.2 geometries.
        errors_lod13 (list[int]): List of val3dity error codes of the LoD1.3 geometries.
        errors_lod22 (list[int]): List of val3dity error codes of the LoD2.2 geometries.
        download (str): The URL of the file download.
        sha256 (str): The SHA256 of the zipfile.
    """

    zip_ok: bool = None
    file_ok: bool = None
    nr_building: int = None
    nr_buildingpart: int = None
    nr_invalid_building: int = None
    nr_invalid_buildingpart_lod12: int = None
    nr_invalid_buildingpart_lod13: int = None
    nr_invalid_buildingpart_lod22: int = None
    errors_lod12: list[int] = None
    errors_lod13: list[int] = None
    errors_lod22: list[int] = None
    download: str = None
    sha256: str = None

    def asdict(self) -> dict:
        return {f"obj_{k}": v for k, v in self.__dict__.items()}


@dataclass
class GPKGFileResults:
    """Results of the compressed GPKG file validation for a single tile.

    Attributes:
        zip_ok (bool): Whether the file is successfully compressed.
        file_ok (bool): Whether the GeoPackage file itself is valid.
        nr_building (int): Number of building features.
        nr_buildingpart (int): Number of building part features.
        attributes_with_errors (AttributeValidationResults): List of attribute names with the error codes that they have.
        download (str): The URL of the file download.
        sha256 (str): The SHA256 of the zipfile.
    """

    zip_ok: bool = None
    file_ok: bool = None
    nr_building: int = None
    nr_buildingpart: int = None
    nr_invalid_2d_geom: int = None
    attributes_with_errors: AttributeValidationResults = field(
        default_factory=AttributeValidationResults
    )
    download: str = None
    sha256: str = None

    def asdict(self) -> dict:
        return {f"gpkg_{k}": v for k, v in self.__dict__.items()}


@dataclass
class TileResults:
    """Results of the validation of each compressed file for the given tile.

    Attributes:
        tile_id (str): The tile ID.
        cityjson (CityJSONFileResults): CityJSON file validation results.
        obj (OBJFileResults): OBJ file validation results.
        gpkg (GPKGFileResults): GPKG file validation results.
    """

    tile_id: str = None
    cityjson: CityJSONFileResults = field(default_factory=CityJSONFileResults)
    obj: OBJFileResults = field(default_factory=OBJFileResults)
    gpkg: GPKGFileResults = field(default_factory=GPKGFileResults)

    def fieldnames(self) -> list[str]:
        return [
            "tile_id",
            *self.cityjson.asdict().keys(),
            *self.obj.asdict().keys(),
            *self.gpkg.asdict().keys(),
        ]

    def asdict(self) -> dict:
        return {
            "tile_id": self.tile_id,
            **self.cityjson.asdict(),
            **self.obj.asdict(),
            **self.gpkg.asdict(),
        }


def cityobject_validate_attributes(
    specs: Specs3DBAGResource, co: dict
) -> Generator[AttributeValidationResultOne, None, None]:
    """Validate the attributes of a CityObject against the 3DBAG attributes specs.

    Args:
        specs (Specs3DBAGResource): The 3DBAG specifications
        co (dict): A single CityObject
    Returns:
        A list of `AttributeValidationResultOne`.
    """
    # CityObject attributes
    if co_attributes := co.get("attributes"):
        building_attributes = dict(
            specs.applies_to(
                data_format="cityjson",
                locations=(CityJSONLocation.from_string(co["type"]),),
            )
        )
        co_diff_specs = set(co_attributes).difference(building_attributes)
        if len(co_diff_specs) > 0:
            yield AttributeValidationResultOne(
                attribute_name=",".join(co_diff_specs),
                outcome=AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES,
            )

        specs_diff_co = set(building_attributes).difference(co_attributes)
        if len(specs_diff_co) > 0:
            yield AttributeValidationResultOne(
                attribute_name=",".join(specs_diff_co),
                outcome=AttributeValidationOutcome.BUILDING_MISSING_ATTRIBUTES,
            )
        for specs_attr in building_attributes.values():
            if co_attr := co_attributes.get(specs_attr.name):
                if type(co_attr).__name__ != specs_attr.type.as_python():
                    yield AttributeValidationResultOne(
                        attribute_name=specs_attr.name,
                        outcome=AttributeValidationOutcome.INCORRECT_DATA_TYPE,
                    )

    # Semantic attributes
    if geometries := co.get("geometry"):
        for geometry in geometries:
            if semantics := geometry.get("semantics"):
                for semantic_surface in semantics["surfaces"]:
                    specs_surface_attributes = dict(
                        specs.applies_to(
                            data_format="cityjson",
                            locations=(
                                CityJSONLocation.from_string(semantic_surface["type"]),
                            ),
                        )
                    )
                    semantic_surface_attributes = {
                        k: v
                        for k, v in semantic_surface.items()
                        if k != "type" and k != "children" and k != "parent"
                    }
                    surface_diff_specs = set(semantic_surface_attributes).difference(
                        specs_surface_attributes
                    )
                    if len(surface_diff_specs) > 0:
                        yield AttributeValidationResultOne(
                            attribute_name=",".join(surface_diff_specs),
                            outcome=AttributeValidationOutcome.SURFACE_EXTRA_ATTRIBUTES,
                        )

                    specs_diff_surface = set(specs_surface_attributes).difference(
                        semantic_surface_attributes
                    )
                    if len(specs_diff_surface) > 0:
                        yield AttributeValidationResultOne(
                            attribute_name=",".join(specs_diff_surface),
                            outcome=AttributeValidationOutcome.SURFACE_MISSING_ATTRIBUTES,
                        )
                    for specs_attr in specs_surface_attributes.values():
                        if sem_attr := semantic_surface_attributes.get(specs_attr.name):
                            if type(sem_attr).__name__ != specs_attr.type.as_python():
                                yield AttributeValidationResultOne(
                                    attribute_name=specs_attr.name,
                                    outcome=AttributeValidationOutcome.INCORRECT_DATA_TYPE,
                                )


def cityjson(
    validation: AppImage,
    dirpath: Path,
    file_id: str,
    planarity_n_tol: float,
    planarity_d2p_tol: float,
    snap_tol: float,
    url_root: str,
    version: str,
    specs: Specs3DBAGResource,
) -> CityJSONFileResults:
    """Validate a single CityJSON file.

    Args:
        validation: Validation resource
        dirpath: Directory with the compressed cityjson file
        file_id: File name without extension
        planarity_n_tol: Val3dity ``planarity_n_tol`` parameter
        planarity_d2p_tol: Val3dity ``planarity_d2p_tol`` parameter
        snap_tol: Val3dity ``snap_tol`` parameter
        url_root: 3DBAG download page url root
        version: 3DBAG version
        specs: 3DBAG specifications resource

    Returns: The aggregated validation results. See ``CityJSONFileResults`` for details.
    """
    results = CityJSONFileResults()
    inputzipfile = dirpath.joinpath(file_id).with_suffix(".city.json.gz")
    inputfile = dirpath / f"{file_id}.city.json"
    inputfile.unlink(missing_ok=True)  # in case a prev run failed

    # test zip
    try:
        cmd = " ".join(["gunzip", "-t", str(inputzipfile)])
        output, returncode = execute_shell_command_silent(
            shell_command=cmd, cwd=str(dirpath)
        )
        results.zip_ok = True if len(output) == 0 else False
    except Exception:
        logger.error(f"Failed to test zip with file {inputzipfile}")
        inputfile.unlink(missing_ok=True)
        return results

    # unzip
    try:
        cmd = " ".join(["gunzip", "--keep", str(inputzipfile)])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
    except Exception:
        logger.error(f"Failed to unzip file {inputzipfile}")
        inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = " ".join(["sha256sum", str(inputzipfile)])
        output, returncode = execute_shell_command_silent(
            shell_command=cmd, cwd=str(dirpath)
        )
        sha256 = output.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root, format="cityjson", file_id=file_id, version=version
        )
    except Exception:
        logger.error("Failed to compute sha256 or create download link")
        inputfile.unlink(missing_ok=True)
        return results

    # cjio feature and lod count
    try:
        cmd = " ".join(
            [
                "{exe}",
                str(inputfile),
                "info",
                "--long",
            ]
        )
        returncode, output = validation.execute(
            "cjio", command=cmd, local_path=dirpath, silent=True
        )
        try:
            results.nr_building = int(
                re.search(r"(?<=Building \()\d+", output).group(0)
            )
        except Exception:
            logger.warning("Failed to extract number of buildings from output")
            results.nr_building = -1
        try:
            results.nr_buildingpart = int(
                re.search(r"(?<=BuildingPart \()\d+", output).group(0)
            )
        except Exception:
            logger.warning("Failed to extract number of building parts from output")
            results.nr_buildingpart = -1
        try:
            results.lod = ast.literal_eval(re.search(r"(?<=LoD = ).+", output).group(0))
        except Exception:
            logger.warning("Failed to extract LoD from output")
            results.lod = [
                "",
            ]
    except Exception as e:
        logger.error("Failed to run cjio info command.")
        inputfile.unlink(missing_ok=True)
        raise e

    # Read the whole CityJSON again, so that we can match the val3dity errors to the
    # errors in the b3_val3dity attributes. It would be better to combine this with the
    # object and lod count above.
    with inputfile.open("r") as fo:
        cm = json.load(fo)
        cityobjects = cm["CityObjects"]

    # val3dity & attribute validation
    reportfile = dirpath / "report.json"
    logfile = dirpath / "val3dity.log"
    try:
        cmd = " ".join(
            [
                "{exe}",
                "--planarity_n_tol",
                str(planarity_n_tol),
                "--planarity_d2p_tol",
                str(planarity_d2p_tol),
                "--snap_tol",
                str(snap_tol),
                "--report",
                str(reportfile),
                str(inputfile),
            ]
        )

        returncode, output = validation.execute(
            "val3dity", command=cmd, local_path=dirpath, silent=True
        )
        results.file_ok = (
            False if returncode != 0 or "error" in output.lower() else True
        )
        with reportfile.open("r") as fo:
            report = json.load(fo)
            nr_invalid_building = 0
            nr_invalid_lod12 = 0
            nr_invalid_lod13 = 0
            nr_invalid_lod22 = 0
            errors_lod12 = set()
            errors_lod13 = set()
            errors_lod22 = set()
            nr_mismatch_errors_lod12 = 0
            nr_mismatch_errors_lod13 = 0
            nr_mismatch_errors_lod22 = 0
            lod12_idx = 1
            lod13_idx = 2
            lod22_idx = 3
            for feature in report["features"]:
                if feature["validity"] is False:
                    nr_invalid_building += 1
                primitives = feature["primitives"]
                # If we don't have all 4 primitives in the val3dity report, then we
                # assume all of them are invalid, because cannot tell which primitive
                # refers to which LoD in the report
                e12 = None
                e13 = None
                e22 = None
                if len(primitives) == 4:
                    nr_invalid_lod12 += 0 if primitives[lod12_idx]["validity"] else 1
                    nr_invalid_lod13 += 0 if primitives[lod13_idx]["validity"] else 1
                    nr_invalid_lod22 += 0 if primitives[lod22_idx]["validity"] else 1
                    e12 = set(
                        e["code"] for e in feature["primitives"][lod12_idx]["errors"]
                    )
                    e13 = set(
                        e["code"] for e in feature["primitives"][lod13_idx]["errors"]
                    )
                    e22 = set(
                        e["code"] for e in feature["primitives"][lod22_idx]["errors"]
                    )
                    errors_lod12.update(e12)
                    errors_lod13.update(e13)
                    errors_lod22.update(e22)
                else:
                    nr_invalid_lod12 += 1
                    nr_invalid_lod13 += 1
                    nr_invalid_lod22 += 1
                cj_co = cityobjects.get(feature["id"])
                if cj_co:
                    if attributes := cj_co.get("attributes"):
                        if v_lod12 := attributes.get("b3_val3dity_lod12"):
                            if e12 != set(eval(v_lod12)):
                                nr_mismatch_errors_lod12 += 1
                        elif e12 is not None:
                            nr_mismatch_errors_lod12 += 1
                        if v_lod13 := attributes.get("b3_val3dity_lod13"):
                            if e13 != set(eval(v_lod13)):
                                nr_mismatch_errors_lod13 += 1
                        elif e13 is not None:
                            nr_mismatch_errors_lod13 += 1
                        if v_lod22 := attributes.get("b3_val3dity_lod22"):
                            if e22 != set(eval(v_lod22)):
                                nr_mismatch_errors_lod22 += 1
                        elif e22 is not None:
                            nr_mismatch_errors_lod22 += 1
                        for res_one in cityobject_validate_attributes(
                            specs=specs, co=cj_co
                        ):
                            results.attributes_with_errors.add_error(res_one)
            results.nr_invalid_building = nr_invalid_building
            results.nr_invalid_buildingpart_lod12 = nr_invalid_lod12
            results.nr_invalid_buildingpart_lod13 = nr_invalid_lod13
            results.nr_invalid_buildingpart_lod22 = nr_invalid_lod22
            results.errors_lod12 = list(errors_lod12)
            results.errors_lod13 = list(errors_lod13)
            results.errors_lod22 = list(errors_lod22)
            results.nr_mismatch_errors_lod12 = nr_mismatch_errors_lod12
            results.nr_mismatch_errors_lod13 = nr_mismatch_errors_lod13
            results.nr_mismatch_errors_lod22 = nr_mismatch_errors_lod22
    except Exception as e:
        logger.error("Failed to run val3dity command.")
        inputfile.unlink(missing_ok=True)
        raise e
    finally:
        reportfile.unlink()
        logfile.unlink(missing_ok=True)

    # cjval
    try:
        cmd = " ".join(["{exe}", str(inputfile)])
        returncode, output = validation.execute(
            "cjval", command=cmd, local_path=dirpath, silent=True
        )
        pos = output.find("SUMMARY")
        summary = output[pos:]
        results.schema_valid = True if summary.find("valid") > 0 else False
        results.schema_warnings = True if summary.find("warnings") > 0 else False
    except Exception as e:
        logger.error("Failed to run cjval command.")
        inputfile.unlink(missing_ok=True)
        raise e

    # clean up
    inputfile.unlink()
    return results


def obj(
    validation: AppImage,
    dirpath: Path,
    file_id: str,
    planarity_n_tol: float,
    planarity_d2p_tol: float,
    snap_tol: float,
    url_root: str,
    version: str,
) -> OBJFileResults:
    results = OBJFileResults()
    inputzipfile = dirpath.joinpath(f"{file_id}-obj.zip")
    inputfiles = [
        dirpath / f"{file_id}-LoD12-3D.obj",
        dirpath / f"{file_id}-LoD12-3D.obj.mtl",
        dirpath / f"{file_id}-LoD13-3D.obj",
        dirpath / f"{file_id}-LoD13-3D.obj.mtl",
        dirpath / f"{file_id}-LoD22-3D.obj",
        dirpath / f"{file_id}-LoD22-3D.obj.mtl",
    ]
    for inputfile in inputfiles:
        inputfile.unlink(missing_ok=True)

    # test zip
    try:
        cmd = " ".join(["unzip", "-t", str(inputzipfile)])
        output, returncode = execute_shell_command_silent(
            shell_command=cmd, cwd=str(dirpath)
        )
        results.zip_ok = True if output.count("OK") == 6 else False
    except Exception:
        logger.error(f"Failed to test zip with file {inputzipfile}")
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = " ".join(["sha256sum", str(inputzipfile)])
        output, returncode = execute_shell_command_silent(
            shell_command=cmd, cwd=str(dirpath)
        )
        sha256 = output.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root, format="obj", file_id=file_id, version=version
        )
    except Exception:
        logger.error("Failed to compute sha256 or create download link")
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # unzip
    try:
        cmd = " ".join(["unzip", "-o", str(inputzipfile)])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
    except Exception:
        logger.error(f"Failed to test zip with file {inputzipfile}")
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # val3dity
    reportfile = dirpath / "report.json"
    logfile = dirpath / "val3dity.log"
    nr_building_all = []
    nr_buildingpart_all = []
    nr_invalid_building_all = []
    for inputfile in inputfiles:
        if inputfile.suffix == ".obj":
            try:
                building_ids = set()
                buildingpart_ids_temp_until_obj_fix = []
                buildingpart_ids = set()
                with inputfile.open("r") as obj_file:
                    for line in obj_file:
                        bid_match = re.search(r"(?<=o )NL\.IMBAG\.Pand\.\d{16}", line)
                        # For now, the OBJ object IDs do not contain the building part suffix, which
                        # should be fixed. Once they contain the suffix, the same set-setup needs to be
                        # used like with building_ids.
                        bpid_match = re.search(
                            r"(?<=o )NL\.IMBAG\.Pand\.\d{16}-\d+", line
                        )
                        if bid_match:
                            building_ids.add(bid_match.group(0))
                            buildingpart_ids_temp_until_obj_fix.append(
                                bid_match.group(0)
                            )  # this can be removed after the OBJ fix
                        elif bpid_match:
                            buildingpart_ids.add(bpid_match.group(0))
                nr_building_all.append(len(building_ids))
                nr_buildingpart_all.append(len(buildingpart_ids_temp_until_obj_fix))
            except Exception:
                logger.error(
                    f"Failed to read building and building part IDs from {inputfile}"
                )
                inputfile.unlink(missing_ok=True)
                return results
            try:
                cmd = " ".join(
                    [
                        "{exe}",
                        "--planarity_n_tol",
                        str(planarity_n_tol),
                        "--planarity_d2p_tol",
                        str(planarity_d2p_tol),
                        "--snap_tol",
                        str(snap_tol),
                        "--report",
                        str(reportfile),
                        str(inputfile),
                    ]
                )

                returncode, output = validation.execute(
                    "val3dity", command=cmd, local_path=dirpath, silent=True
                )
                results.file_ok = (
                    False if returncode != 0 or "error" in output.lower() else True
                )
                with reportfile.open("r") as fo:
                    report = json.load(fo)

                current_lod = re.search(r"(?<=LoD)\d{2}", inputfile.name).group(0)
                invalid_building_ids = set()
                nr_invalid_lod12 = 0
                nr_invalid_lod13 = 0
                nr_invalid_lod22 = 0
                errors_lod12 = set()
                errors_lod13 = set()
                errors_lod22 = set()
                for feature in report["features"]:
                    for primitive in feature["primitives"]:
                        if primitive["validity"] is False:
                            building_id = primitive["id"][:31]
                            invalid_building_ids.add(building_id)
                            if current_lod == "12":
                                nr_invalid_lod12 += 1
                                for e in primitive["errors"]:
                                    errors_lod12.add(e["code"])
                            elif current_lod == "13":
                                nr_invalid_lod13 += 1
                                for e in primitive["errors"]:
                                    errors_lod13.add(e["code"])
                            elif current_lod == "22":
                                nr_invalid_lod22 += 1
                                for e in primitive["errors"]:
                                    errors_lod22.add(e["code"])
                nr_invalid_building_all.append(len(invalid_building_ids))
                if current_lod == "12":
                    results.nr_invalid_buildingpart_lod12 = nr_invalid_lod12
                    results.errors_lod12 = list(errors_lod12)
                elif current_lod == "13":
                    results.nr_invalid_buildingpart_lod13 = nr_invalid_lod13
                    results.errors_lod13 = list(errors_lod13)
                elif current_lod == "22":
                    results.nr_invalid_buildingpart_lod22 = nr_invalid_lod22
                    results.errors_lod22 = list(errors_lod22)
                reportfile.unlink()
                logfile.unlink(missing_ok=True)
            except Exception as e:
                logger.error("Failed to run val3dity command.")
                reportfile.unlink(missing_ok=True)
                logfile.unlink(missing_ok=True)
                inputfile.unlink(missing_ok=True)
                raise e
    results.nr_building = min(nr_building_all)
    results.nr_buildingpart = min(nr_buildingpart_all)
    results.nr_invalid_building = max(nr_invalid_building_all)

    for inputfile in inputfiles:
        inputfile.unlink()
    return results


def gpkg_validate_attributes(
    specs: Specs3DBAGResource, gpkg_info: dict
) -> Generator[AttributeValidationResultOne, None, None]:
    """Validate the attributes of a GPKG against the 3DBAG attributes specs.

    Args:
        specs (Specs3DBAGResource): 3DBAG specifications
        gpkg_info (dict): The output of OGRInfo in JSON format, deserialized to python

    Returns:
        A list of `AttributeValidationResultOne`.
    """
    for layer in gpkg_info["layers"]:
        gpkg_location = GpkgLocation.from_string(layer["name"])
        specs_attributes = dict(
            specs.applies_to(data_format="gpkg", locations=(gpkg_location,))
        )

        layer_fields = layer["fields"]
        gpkg_field_names = set(f["name"] for f in layer_fields)
        gpkg_diff_specs = gpkg_field_names.difference(specs_attributes)
        if gpkg_location in GpkgLocation.building_layers():
            error_extra_attributes = (
                AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES
            )
            error_missing_attributes = (
                AttributeValidationOutcome.BUILDING_MISSING_ATTRIBUTES
            )
        else:
            error_extra_attributes = AttributeValidationOutcome.SURFACE_EXTRA_ATTRIBUTES
            error_missing_attributes = (
                AttributeValidationOutcome.SURFACE_MISSING_ATTRIBUTES
            )
        # Check for extra/missing attributes
        if len(gpkg_diff_specs) > 0:
            yield AttributeValidationResultOne(
                attribute_name=",".join(gpkg_diff_specs),
                outcome=error_extra_attributes,
            )

        specs_diff_gpkg = set(specs_attributes).difference(gpkg_field_names)
        if len(specs_diff_gpkg) > 0:
            yield AttributeValidationResultOne(
                attribute_name=",".join(specs_diff_gpkg),
                outcome=error_missing_attributes,
            )

        # Check attribute types
        for gpkg_attr in layer_fields:
            a_name = gpkg_attr["name"]
            if spec_attr := specs_attributes.get(a_name):
                if gpkg_attr["type"] != spec_attr.type.as_ogr():
                    yield AttributeValidationResultOne(
                        attribute_name=a_name,
                        outcome=AttributeValidationOutcome.INCORRECT_DATA_TYPE,
                    )

                if gpkg_attr["nullable"] != spec_attr.nullable:
                    yield AttributeValidationResultOne(
                        attribute_name=a_name,
                        outcome=AttributeValidationOutcome.INCORRECT_NULLABLE,
                    )


def gpkg(
    gdal: AppImage,
    dirpath: Path,
    file_id: str,
    url_root: str,
    version: str,
    specs: Specs3DBAGResource,
) -> GPKGFileResults:
    results = GPKGFileResults()
    inputzipfile = dirpath.joinpath(file_id).with_suffix(".gpkg.gz")
    inputfile = dirpath.joinpath(file_id).with_suffix(".gpkg")
    propertiesfile = dirpath.joinpath(file_id).with_suffix(".gpkg.gz.properties")

    # test zip
    try:
        cmd = " ".join(["gunzip", "-t", str(inputzipfile)])
        output, returncode = execute_shell_command_silent(
            shell_command=cmd, cwd=str(dirpath)
        )
        results.zip_ok = True if len(output) == 0 else False
    except Exception:
        logger.error(f"Failed to test zip with file {inputzipfile}")
        return results

    # unzip
    try:
        cmd = " ".join(["gunzip", "--keep", str(inputzipfile)])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
    except Exception:
        logger.error(f"Failed to unzip file {inputzipfile}")
        inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = " ".join(["sha256sum", str(inputzipfile)])
        output, returncode = execute_shell_command_silent(
            shell_command=cmd, cwd=str(dirpath)
        )
        sha256 = output.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root, format="gpkg", file_id=file_id, version=version
        )
    except Exception:
        logger.error("Failed to compute sha256 or create download link")
        return results
    finally:
        inputfile.unlink(missing_ok=True)

    # ogrinfo
    nr_building_all = []
    nr_buildingpart_all = []
    nr_invalid_2d_geom_all = []

    try:
        for layer in ["lod12_3d", "lod13_3d", "lod22_3d"]:
            sql_buildingpart_count = f"-sql 'select count(identificatie) from {layer}'"
            sql_building_count = (
                f"-sql 'select count(distinct identificatie) from {layer}'"
            )

            cmd = " ".join(
                [
                    "LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH",
                    "{exe}",
                    sql_buildingpart_count,
                    f"/vsigzip//{inputzipfile}",
                ]
            )
            returncode, output = gdal.execute(
                "ogrinfo", command=cmd, local_path=dirpath, silent=True
            )
            results.file_ok = (
                False if returncode != 0 or "error" in output.lower() else True
            )
            re_buildingpart_count = r"(?<=count\(identificatie\) \(Integer\) = )\d+"

            try:
                n = int(re.search(re_buildingpart_count, output).group(0))
                nr_buildingpart_all.append(n)

            except Exception:
                logger.warning(
                    f"Failed to extract number of building parts from output for layer {layer}"
                )
                n = None

            cmd = " ".join(
                [
                    "LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH",
                    "{exe}",
                    sql_building_count,
                    f"/vsigzip//{inputzipfile}",
                ]
            )
            returncode, output = gdal.execute(
                "ogrinfo", command=cmd, local_path=dirpath, silent=True
            )
            re_building_count = (
                r"(?<=count\(distinct identificatie\) \(Integer\) = )\d+"
            )
            try:
                n = int(re.search(re_building_count, output).group(0))
                nr_building_all.append(n)
            except Exception:
                logger.warning(
                    f"Failed to extract number of buildings from output for layer {layer}"
                )
                n = None
        for layer in ["lod12_2d", "lod13_2d", "lod22_2d"]:
            sql_invalid_geom_count = f"""-sql 'SELECT COUNT(DISTINCT identificatie) as invalid_count FROM {layer} WHERE identificatie IN (SELECT identificatie FROM {layer} WHERE ST_IsValid(geom) = false)'"""

            cmd = " ".join(
                [
                    "LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH",
                    "{exe}",
                    sql_invalid_geom_count,
                    f"/vsigzip//{inputzipfile}",
                ]
            )
            returncode, output = gdal.execute(
                "ogrinfo", command=cmd, local_path=dirpath, silent=True
            )
            re_invalid_count = r"(?<=invalid_count \(Integer\) = )\d+"
            try:
                n = int(re.search(re_invalid_count, output).group(0))
                nr_invalid_2d_geom_all.append(n)
            except Exception:
                logger.warning(
                    f"Failed to extract number of valid geometries from output for layer {layer}"
                )
                n = None
        # Attribute validation
        cmd = " ".join(
            [
                "LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH",
                "{exe}",
                "-so",
                "-json",
                f"/vsigzip//{inputzipfile}",
            ]
        )
        returncode, output = gdal.execute(
            "ogrinfo", command=cmd, local_path=dirpath, silent=True
        )
        try:
            gpkg_info = json.loads(output)
            for res_one in gpkg_validate_attributes(specs=specs, gpkg_info=gpkg_info):
                results.attributes_with_errors.add_error(res_one)
        except Exception:
            logger.warning("Failed to get the json ogrinfo for file")
    except Exception as e:
        logger.error("Failed to run validation for gpkg")
        raise e
    results.nr_building = min(nr_building_all)
    results.nr_buildingpart = min(nr_buildingpart_all)
    results.nr_invalid_2d_geom = min(nr_invalid_2d_geom_all)
    propertiesfile.unlink(missing_ok=True)
    return results


def create_download_link(url_root: str, format: str, file_id: str, version: str) -> str:
    tile_id = file_id.replace("-", "/")
    version_stripped = version.replace(".", "")
    if format == "cityjson":
        filename = f"{file_id}.city.json.gz"
        link = f"{url_root}/{version_stripped}/tiles/{tile_id}/{filename}"
    elif format == "gpkg":
        filename = f"{file_id}.gpkg.gz"
        link = f"{url_root}/{version_stripped}/tiles/{tile_id}/{filename}"
    elif format == "obj":
        filename = f"{file_id}-obj.zip"
        link = f"{url_root}/{version_stripped}/tiles/{tile_id}/{filename}"
    else:
        raise ValueError(f"only cityjson, obj, gpkg format is allowed, got {format}")
    return link


def check_formats(input) -> TileResults:
    gdal, validation, dirpath, tile_id, url_root, version, specs = input
    file_id = tile_id.replace("/", "-")
    planarity_n_tol = 20.0
    planarity_d2p_tol = 0.001
    snap_tol = 0.0001
    cj_results = cityjson(
        validation=validation,
        dirpath=dirpath,
        file_id=file_id,
        planarity_n_tol=planarity_n_tol,
        planarity_d2p_tol=planarity_d2p_tol,
        snap_tol=snap_tol,
        url_root=url_root,
        version=version,
        specs=specs,
    )
    obj_results = obj(
        validation,
        dirpath,
        file_id,
        planarity_n_tol=planarity_n_tol,
        planarity_d2p_tol=planarity_d2p_tol,
        snap_tol=snap_tol,
        url_root=url_root,
        version=version,
    )
    gpkg_results = gpkg(
        gdal, dirpath, file_id, url_root=url_root, version=version, specs=specs
    )
    return TileResults(tile_id, cj_results, obj_results, gpkg_results)


@asset(
    ins={
        "export_index": AssetIn(key_prefix="export"),
        "metadata": AssetIn(key_prefix="export"),
    },
    deps=[AssetKey(("export", "compressed_tiles"))],
    required_resource_keys={"file_store", "version", "gdal", "validation", "specs"},
)
def compressed_tiles_validation(
    context: OpExecutionContext, export_index: Path, metadata: Path
) -> Path:
    """Validates the compressed distribution tiles, for each format.
    Save the validation results to a CSV.
    Validation is done concurrently per tile.

    Validation:

    - check if the archive is valid
    - compute the SHA-256 of the archive
    - add the download links per format
    - number of buildings and building parts per format
    - run val3dity on the CityJSON and OBJ formats and record the number of invalids
        and the error codes per LoD
    - compare the CityJSON validity attribute error codes to those computed from the
        file directly
    - CityJSON schema validation
    - CityJSON LoD-s present in the file

    The computed attributes are described at the members of the TileResults class.
    """
    path_export_dir = bag3d_export_dir(
        context.resources.file_store.file_store.data_dir,
        version=context.resources.version.version,
    )
    url_root = "https://data.3dbag.nl"
    with metadata.open("r") as fo:
        metadata_json = json.load(fo)
        version = metadata_json["identificationInfo"]["citation"]["edition"]
        context.log.debug(f"{version=}")
    gdal = context.resources.gdal.app
    validation = context.resources.validation.app
    specs = context.resources.specs
    with export_index.open("r") as fo:
        csvreader = csv.reader(fo)
        _ = next(csvreader)  # header
        tileids = [
            (
                gdal,
                validation,
                path_export_dir.joinpath("tiles", row[0]),
                row[0],
                url_root,
                version,
                specs,
            )
            for row in csvreader
        ]

    output_path = path_export_dir.joinpath("validate_compressed_files.csv")
    fo = output_path.open("w")
    csvwriter = csv.DictWriter(
        fo, quoting=csv.QUOTE_NONNUMERIC, fieldnames=TileResults().fieldnames()
    )
    csvwriter.writeheader()

    try:
        for tileid in tileids:
            tile_result = check_formats(tileid)
            csvwriter.writerow(tile_result.asdict())
    finally:
        fo.close()
    # try:
    #     with ProcessPoolExecutor() as executor:
    #         for result in executor.map(check_formats, tileids):
    #             csvwriter.writerow(result.asdict())
    # finally:
    #     fo.close()

    return output_path
