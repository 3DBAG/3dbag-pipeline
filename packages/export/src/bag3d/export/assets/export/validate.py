import csv
import gzip
import hashlib
import json
import re
import shutil
import zipfile
from collections.abc import Generator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory

from bag3d.common.resources.executables import (
    CommandRunner,
    GDALResource,
    ValidationResource,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.version import ReleaseVersionResource
from dagster import AssetIn, AssetKey, Config, asset, get_dagster_logger
from pydantic import Field

from bag3d.export.assets.export.attributes import (
    AttributeValidationResults,
    cityobject_validate_attributes,
    gpkg_validate_attributes,
)
from bag3d.export.assets.export.inspect_geopackage import inspect_geopackage
from bag3d.export.assets.export.inspect_tiles import inspect_tileset

logger = get_dagster_logger("export.validate")


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

    zip_ok: bool | None = None
    file_ok: bool | None = None
    nr_building: int | None = None
    nr_buildingpart: int | None = None
    nr_invalid_building: int | None = None
    nr_invalid_buildingpart_lod12: int | None = None
    nr_invalid_buildingpart_lod13: int | None = None
    nr_invalid_buildingpart_lod22: int | None = None
    errors_lod12: list[int] | None = None
    errors_lod13: list[int] | None = None
    errors_lod22: list[int] | None = None
    nr_mismatch_errors_lod12: int | None = None
    nr_mismatch_errors_lod13: int | None = None
    nr_mismatch_errors_lod22: int | None = None
    lod: list[str] | None = None
    schema_valid: bool | None = None
    schema_warnings: bool | None = None
    attributes_with_errors: AttributeValidationResults = field(
        default_factory=AttributeValidationResults
    )
    download: str | None = None
    sha256: str | None = None

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

    zip_ok: bool | None = None
    file_ok: bool | None = None
    nr_building: int | None = None
    nr_buildingpart: int | None = None
    nr_invalid_building: int | None = None
    nr_invalid_buildingpart_lod12: int | None = None
    nr_invalid_buildingpart_lod13: int | None = None
    nr_invalid_buildingpart_lod22: int | None = None
    errors_lod12: list[int] | None = None
    errors_lod13: list[int] | None = None
    errors_lod22: list[int] | None = None
    download: str | None = None
    sha256: str | None = None

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

    zip_ok: bool | None = None
    file_ok: bool | None = None
    nr_building: int | None = None
    nr_buildingpart: int | None = None
    nr_invalid_2d_geom: int | None = None
    attributes_with_errors: AttributeValidationResults = field(
        default_factory=AttributeValidationResults
    )
    download: str | None = None
    sha256: str | None = None

    def asdict(self) -> dict:
        return {f"gpkg_{k}": v for k, v in self.__dict__.items()}


@dataclass
class IFCFileResults:
    """Results of the IFC archive validation for a single tile.

    Attributes:
        zip_ok (bool): Whether the archive is valid and contains the expected
            number of IFC files (one per LoD).
        nr_ifc_files (int): Number of IFC files in the archive.
        download (str): The URL of the file download.
        sha256 (str): The SHA256 of the zipfile.
    """

    zip_ok: bool | None = None
    nr_ifc_files: int | None = None
    download: str | None = None
    sha256: str | None = None

    def asdict(self) -> dict:
        return {f"ifc_{k}": v for k, v in self.__dict__.items()}


@dataclass
class TileResults:
    """Results of the validation of each compressed file for the given tile.

    Attributes:
        tile_id (str): The tile ID.
        cityjson (CityJSONFileResults): CityJSON file validation results.
        obj (OBJFileResults): OBJ file validation results.
        gpkg (GPKGFileResults): GPKG file validation results.
        ifc (IFCFileResults): IFC archive validation results.
    """

    tile_id: str | None = None
    cityjson: CityJSONFileResults = field(default_factory=CityJSONFileResults)
    obj: OBJFileResults = field(default_factory=OBJFileResults)
    gpkg: GPKGFileResults = field(default_factory=GPKGFileResults)
    ifc: IFCFileResults = field(default_factory=IFCFileResults)

    def fieldnames(self) -> list[str]:
        return [
            "tile_id",
            *self.cityjson.asdict().keys(),
            *self.obj.asdict().keys(),
            *self.gpkg.asdict().keys(),
            *self.ifc.asdict().keys(),
        ]

    def asdict(self) -> dict:
        return {
            "tile_id": self.tile_id,
            **self.cityjson.asdict(),
            **self.obj.asdict(),
            **self.gpkg.asdict(),
            **self.ifc.asdict(),
        }


def cityjson(
    system: CommandRunner,
    validation_runner: CommandRunner,
    dirpath: Path,
    file_id: str,
    planarity_n_tol: float,
    planarity_d2p_tol: float,
    snap_tol: float,
    url_root: str,
    version: str,
    specs: Specs3DBAGResource,
    url_file_id: str | None = None,
) -> CityJSONFileResults:
    """Validate a single CityJSON file.

    Args:
        system: CommandRunner for system tools
        validation_runner: CommandRunner for validation tools
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
        result = system.run(cmd, cwd=str(dirpath))
        results.zip_ok = bool(result.success and len(result.stdout) == 0)
    except Exception:  # noqa: BLE001
        logger.error(f"Failed to test zip with file {inputzipfile}")
        inputfile.unlink(missing_ok=True)
        return results

    if not results.zip_ok:
        results.file_ok = False
        return results

    # unzip
    try:
        cmd = " ".join(["gunzip", "--keep", str(inputzipfile)])
        system.run(cmd, cwd=str(dirpath))
    except Exception:  # noqa: BLE001
        logger.error(f"Failed to unzip file {inputzipfile}")
        inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = " ".join(["sha256sum", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        sha256 = result.stdout.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root,
            format="cityjson",
            file_id=url_file_id or file_id,
            version=version,
        )
    except Exception:  # noqa: BLE001
        logger.error("Failed to compute sha256 or create download link")
        inputfile.unlink(missing_ok=True)
        return results

    with inputfile.open("r", encoding="utf-8") as fo:
        cm = json.load(fo)
    cityobjects = cm["CityObjects"]
    results.nr_building = sum(co["type"] == "Building" for co in cityobjects.values())
    results.nr_buildingpart = sum(
        co["type"] == "BuildingPart" for co in cityobjects.values()
    )
    results.lod = sorted(
        {
            str(g["lod"])
            for co in cityobjects.values()
            for g in co.get("geometry", [])
            if "lod" in g
        }
    )
    for co in cityobjects.values():
        for outcome in cityobject_validate_attributes(specs, co):
            results.attributes_with_errors.add_error(outcome)

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

        result = validation_runner.run(cmd, exe_name="val3dity", local_path=dirpath)
        results.file_ok = not (not result.success or "error" in result.stdout.lower())
        with reportfile.open("r", encoding="utf-8") as fo:
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
                    e12 = {
                        e["code"] for e in feature["primitives"][lod12_idx]["errors"]
                    }
                    e13 = {
                        e["code"] for e in feature["primitives"][lod13_idx]["errors"]
                    }
                    e22 = {
                        e["code"] for e in feature["primitives"][lod22_idx]["errors"]
                    }
                    errors_lod12.update(e12)
                    errors_lod13.update(e13)
                    errors_lod22.update(e22)
                else:
                    nr_invalid_lod12 += 1
                    nr_invalid_lod13 += 1
                    nr_invalid_lod22 += 1
                cj_co = cityobjects.get(feature["id"])
                if cj_co and (attributes := cj_co.get("attributes")):
                    if v_lod12 := attributes.get("b3_val3dity_lod12"):
                        if e12 != set(
                            json.loads(v_lod12) if isinstance(v_lod12, str) else v_lod12
                        ):
                            nr_mismatch_errors_lod12 += 1
                    elif e12 is not None:
                        nr_mismatch_errors_lod12 += 1
                    if v_lod13 := attributes.get("b3_val3dity_lod13"):
                        if e13 != set(
                            json.loads(v_lod13) if isinstance(v_lod13, str) else v_lod13
                        ):
                            nr_mismatch_errors_lod13 += 1
                    elif e13 is not None:
                        nr_mismatch_errors_lod13 += 1
                    if v_lod22 := attributes.get("b3_val3dity_lod22"):
                        if e22 != set(
                            json.loads(v_lod22) if isinstance(v_lod22, str) else v_lod22
                        ):
                            nr_mismatch_errors_lod22 += 1
                    elif e22 is not None:
                        nr_mismatch_errors_lod22 += 1
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
    except Exception:
        logger.error("Failed to run val3dity command.")
        inputfile.unlink(missing_ok=True)
        raise
    finally:
        reportfile.unlink(missing_ok=True)
        logfile.unlink(missing_ok=True)

    # cjval
    try:
        cmd = " ".join(["{exe}", str(inputfile), "-v"])
        result = validation_runner.run(cmd, exe_name="cjval", local_path=dirpath)
        pos = result.stdout.find("SUMMARY")
        summary = result.stdout[pos:]
        results.schema_valid = summary.find("valid") > 0
        results.schema_warnings = summary.find("warnings") > 0
    except Exception:
        logger.error("Failed to run cjval command.")
        inputfile.unlink(missing_ok=True)
        raise

    results.file_ok = bool(
        results.file_ok
        and results.schema_valid
        and results.attributes_with_errors.all_ok()
    )
    # clean up
    inputfile.unlink()
    return results


def obj(
    system: CommandRunner,
    validation_runner: CommandRunner,
    dirpath: Path,
    file_id: str,
    planarity_n_tol: float,
    planarity_d2p_tol: float,
    snap_tol: float,
    url_root: str,
    version: str,
    archive_file_id: str | None = None,
    url_file_id: str | None = None,
) -> OBJFileResults:
    results = OBJFileResults()
    inputzipfile = dirpath.joinpath(f"{archive_file_id or file_id}-obj.zip")
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
        result = system.run(cmd, cwd=str(dirpath))
        results.zip_ok = result.stdout.count("OK") == 6
    except Exception:  # noqa: BLE001
        logger.error(f"Failed to test zip with file {inputzipfile}")
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = " ".join(["sha256sum", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        sha256 = result.stdout.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root,
            format="obj",
            file_id=url_file_id or file_id,
            version=version,
        )
    except Exception:  # noqa: BLE001
        logger.error("Failed to compute sha256 or create download link")
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # unzip
    try:
        cmd = " ".join(["unzip", "-o", str(inputzipfile)])
        system.run(cmd, cwd=str(dirpath))
    except Exception:  # noqa: BLE001
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
                with inputfile.open("r", encoding="utf-8") as obj_file:
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
            except Exception:  # noqa: BLE001
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

                result = validation_runner.run(
                    cmd, exe_name="val3dity", local_path=dirpath
                )
                current_ok = result.success and "error" not in result.stdout.lower()
                results.file_ok = (
                    current_ok
                    if results.file_ok is None
                    else results.file_ok and current_ok
                )
                with reportfile.open("r", encoding="utf-8") as fo:
                    report = json.load(fo)

                current_lod = re.search(r"(?<=LoD)\d{2}", inputfile.name).group(0)  # type: ignore[union-attr]
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
                reportfile.unlink(missing_ok=True)
                logfile.unlink(missing_ok=True)
            except Exception as e:
                logger.error("Failed to run val3dity command.")
                reportfile.unlink(missing_ok=True)
                logfile.unlink(missing_ok=True)
                inputfile.unlink(missing_ok=True)
                raise
    if nr_building_all and nr_buildingpart_all and nr_invalid_building_all:
        results.nr_building = min(nr_building_all)
        results.nr_buildingpart = min(nr_buildingpart_all)
        results.nr_invalid_building = max(nr_invalid_building_all)
    else:
        results.file_ok = False
        logger.error(f"Tile {file_id}, OBJ: empty geometry/count results in {dirpath}")

    for inputfile in inputfiles:
        inputfile.unlink()
    return results


def gpkg(
    system: CommandRunner,
    gdal_runner: CommandRunner,
    dirpath: Path,
    file_id: str,
    url_root: str,
    version: str,
    specs: Specs3DBAGResource,
    url_file_id: str | None = None,
) -> GPKGFileResults:
    results = GPKGFileResults()
    archive = dirpath / f"{file_id}.gpkg.gz"
    results.download = create_download_link(
        url_root, "gpkg", url_file_id or file_id, version
    )
    with archive.open("rb") as stream:
        results.sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    # A private directory avoids clobbering an uncompressed export on retries.
    with TemporaryDirectory(prefix=f".validate-{file_id}-", dir=dirpath) as temporary:
        path = Path(temporary) / f"{file_id}.gpkg"
        with gzip.open(archive, "rb") as source, path.open("wb") as target:
            shutil.copyfileobj(source, target)
        results.zip_ok = True
        info, counts, failures = inspect_geopackage(path, specs, gdal_runner)
        for outcome in gpkg_validate_attributes(specs, info):
            results.attributes_with_errors.add_error(outcome)
        for key, count in counts.items():
            setattr(results, key, count)
        for failure in failures:
            logger.error(f"Tile {file_id}, GeoPackage: {failure}")
        results.file_ok = not failures and results.attributes_with_errors.all_ok()
    return results


def ifc(
    system: CommandRunner,
    dirpath: Path,
    file_id: str,
    url_root: str,
    version: str,
    archive_file_id: str | None = None,
    url_file_id: str | None = None,
) -> IFCFileResults:
    """Validate a single IFC archive.

    Args:
        system: CommandRunner for system tools
        dirpath: Directory with the IFC archive
        file_id: File name without extension
        url_root: 3DBAG download page url root
        version: 3DBAG version

    Returns: The aggregated validation results. See ``IFCFileResults`` for details.
    """
    results = IFCFileResults()
    inputzipfile = dirpath.joinpath(f"{archive_file_id or file_id}-ifc.zip")

    # test zip
    try:
        cmd = " ".join(["unzip", "-t", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        results.zip_ok = result.stdout.count("OK") == 4
    except Exception:  # noqa: BLE001
        logger.error(f"Failed to test zip with file {inputzipfile}")
        return results

    # download link and sha256
    try:
        cmd = " ".join(["sha256sum", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        results.sha256 = result.stdout.split(" ")[0]
        results.download = create_download_link(
            url_root=url_root,
            format="ifc",
            file_id=url_file_id or file_id,
            version=version,
        )
    except Exception:  # noqa: BLE001
        logger.error("Failed to compute sha256 or create download link")
        return results

    # number of IFC files in the archive
    try:
        with zipfile.ZipFile(inputzipfile) as zf:
            results.nr_ifc_files = len(zf.namelist())
    except Exception:  # noqa: BLE001
        logger.error(f"Failed to read zip {inputzipfile}")

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
    elif format == "ifc":
        filename = f"{file_id}-ifc.zip"
        link = f"{url_root}/{version_stripped}/tiles/{tile_id}/{filename}"
    else:
        raise ValueError(
            f"only cityjson, obj, gpkg, ifc format is allowed, got {format}"
        )
    return link


def _check_formats(inputs) -> TileResults:
    """Worker function - no Dagster context available."""
    validation_runner, gdal_runner, dirpath, tile_id, url_root, version = inputs

    # System tools runner (no configured exes needed)
    system = CommandRunner()

    specs = Specs3DBAGResource()
    file_id = tile_id.rsplit("/", 1)[-1]
    planarity_n_tol = 20.0
    planarity_d2p_tol = 0.0001
    snap_tol = 0.0001
    cj_results = cityjson(
        system=system,
        validation_runner=validation_runner,
        dirpath=dirpath,
        file_id=file_id,
        planarity_n_tol=planarity_n_tol,
        planarity_d2p_tol=planarity_d2p_tol,
        snap_tol=snap_tol,
        url_root=url_root,
        version=version,
        specs=specs,
        url_file_id=tile_id.replace("/", "-"),
    )
    obj_results = obj(
        system,
        validation_runner,
        dirpath,
        file_id,
        planarity_n_tol=planarity_n_tol,
        planarity_d2p_tol=planarity_d2p_tol,
        snap_tol=snap_tol,
        url_root=url_root,
        version=version,
        archive_file_id=tile_id.replace("/", "-"),
        url_file_id=tile_id.replace("/", "-"),
    )
    gpkg_results = gpkg(
        system,
        gdal_runner,
        dirpath,
        file_id,
        url_root=url_root,
        version=version,
        specs=specs,
        url_file_id=tile_id.replace("/", "-"),
    )
    ifc_results = ifc(
        system,
        dirpath,
        file_id,
        url_root=url_root,
        version=version,
        archive_file_id=tile_id.replace("/", "-"),
        url_file_id=tile_id.replace("/", "-"),
    )
    return TileResults(tile_id, cj_results, obj_results, gpkg_results, ifc_results)


def check_formats(inputs) -> TileResults:
    try:
        return _check_formats(inputs)
    except Exception as exc:
        raise RuntimeError(
            f"Tile {inputs[3]}: serialized artifact validation failed"
        ) from exc


class ValidationConfig(Config):
    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_VALIDATION", "4")),
        description="ProcessPoolExecutor max_workers for validation",
    )


@asset(
    ins={
        "export_index": AssetIn(key_prefix="export"),
        "metadata": AssetIn(key_prefix="export"),
        "reconstruction_output_3dtiles_lod12": AssetIn(key_prefix="export"),
        "reconstruction_output_3dtiles_lod13": AssetIn(key_prefix="export"),
        "reconstruction_output_3dtiles_lod22": AssetIn(key_prefix="export"),
    },
    deps=[AssetKey(("export", "compressed_tiles"))],
    pool="validation",
)
def compressed_tiles_validation(
    config: ValidationConfig,
    export_index: Path,
    metadata: Path,
    reconstruction_output_3dtiles_lod12: Path,
    reconstruction_output_3dtiles_lod13: Path,
    reconstruction_output_3dtiles_lod22: Path,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    gdal: GDALResource,
    validation: ValidationResource,
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
    path_export_dir = file_store.stage_subdir("export", version.version)
    url_root = "https://data.3dbag.nl"
    with metadata.open("r", encoding="utf-8") as fo:
        metadata_json = json.load(fo)
        version_str = metadata_json["identificationInfo"]["citation"]["edition"]
        logger.debug(f"{version_str=}")
    validation_runner = validation.runner
    gdal_runner = gdal.runner
    with export_index.open("r", encoding="utf-8") as fo:
        csvreader = csv.reader(fo)
        _ = next(csvreader)  # header
        tileids = [
            (
                validation_runner,
                gdal_runner,
                path_export_dir.joinpath("t", row[0]).parent,
                row[0],
                url_root,
                version_str,
            )
            for row in csvreader
        ]

    specs = Specs3DBAGResource()
    with (path_export_dir / "validate_3dtiles.csv").open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.writer(stream)
        writer.writerow(["lod", "attribute", "outcome"])
        for lod, directory in (
            ("lod12", reconstruction_output_3dtiles_lod12),
            ("lod13", reconstruction_output_3dtiles_lod13),
            ("lod22", reconstruction_output_3dtiles_lod22),
        ):
            outcomes = set(inspect_tileset(directory, lod, specs))
            if outcomes:
                writer.writerows(
                    (lod, result.attribute_name, result.outcome.name)
                    for result in sorted(
                        outcomes,
                        key=lambda result: (
                            result.attribute_name,
                            result.outcome.value,
                        ),
                    )
                )
            else:
                writer.writerow([lod, "", "OK"])

    output_path = path_export_dir.joinpath("validate_compressed_files.csv")
    fo = output_path.open("w", encoding="utf-8", newline="")
    csvwriter = csv.DictWriter(
        fo, quoting=csv.QUOTE_NONNUMERIC, fieldnames=TileResults().fieldnames()
    )
    csvwriter.writeheader()

    try:
        with ProcessPoolExecutor(max_workers=config.concurrency) as executor:
            for result in executor.map(check_formats, tileids):
                csvwriter.writerow(result.asdict())
        return output_path
    finally:
        fo.close()
