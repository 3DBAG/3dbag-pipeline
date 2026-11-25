import csv
import gzip
import hashlib
import json
import re
import shlex
import shutil
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import quote

from bag3d.common.resources.executables import (
    CommandResult,
    CommandRunner,
    GDALResource,
    ValidationResource,
)
from bag3d.common.resources.files import (
    DEFAULT_TILES_DIR_NAME,
    FileStoreResource,
    TilesDirResource,
)
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.version import ReleaseVersionResource
from dagster import (
    AssetIn,
    AssetKey,
    Config,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
)
from pydantic import Field

from bag3d.export.assets.export.attributes import (
    AttributeValidationResults,
    cityobject_validate_attributes,
    gpkg_validate_attributes,
    lod_error_codes,
)
from bag3d.export.assets.export.inspect_geopackage import inspect_geopackage
from bag3d.export.assets.export.inspect_tiles import inspect_tileset
from bag3d.export.assets.export.metadata import (
    read_export_edition,
    read_indexed_tile_ids,
)

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


def _matches_lod_report(value: object, expected: set[int] | None) -> bool:
    if value is None:
        return expected is None
    codes = lod_error_codes(value)
    return codes is not None and set(codes) == expected


def _record_cityjson_report(
    results: CityJSONFileResults,
    report: dict,
    cityobjects: dict,
) -> None:
    """Join val3dity's flat geometry error IDs to the source CityJSON LoDs."""
    lod_suffixes = {"1.2": "12", "1.3": "13", "2.2": "22"}
    invalid = Counter()
    mismatches = Counter()
    tile_codes: dict[str, set[int]] = {lod: set() for lod in lod_suffixes}
    results.nr_invalid_building = 0
    for feature in report["features"]:
        if feature["type"] != "Building":
            continue
        fid = feature["id"]
        building = cityobjects[fid]
        attrs = building.get("attributes", {})
        results.nr_invalid_building += int(not feature["validity"])
        # Shell/face errors identify one geometry. BuildingPart overlap identifies both.
        geometry_errors: dict[tuple[str, int], set[int]] = defaultdict(set)
        for error in feature["errors"]:
            for oid, index in re.findall(r"coid=([^|]+)\|geom=(\d+)", error["id"]):
                geometry_errors[oid, int(index)].add(error["code"])

        building_codes: dict[str, set[int]] = {}
        for oid in (fid, *building.get("children", [])):
            for index, geom in enumerate(cityobjects[oid].get("geometry", [])):
                lod = str(geom["lod"])
                if lod in lod_suffixes:
                    codes = geometry_errors.get((oid, index), set())
                    building_codes.setdefault(lod, set()).update(codes)
                    invalid[lod] += int(bool(codes))
        for lod, suffix in lod_suffixes.items():
            expected = building_codes.get(lod)
            if expected is None:
                # As in the existing release metrics, count a missing LoD as invalid.
                invalid[lod] += 1
            else:
                tile_codes[lod].update(expected)
            if attrs and not _matches_lod_report(
                attrs.get(f"b3_val3dity_lod{suffix}"), expected
            ):
                mismatches[lod] += 1

    for lod, suffix in lod_suffixes.items():
        setattr(results, f"nr_invalid_buildingpart_lod{suffix}", invalid[lod])
        setattr(results, f"errors_lod{suffix}", sorted(tile_codes[lod]))
        setattr(results, f"nr_mismatch_errors_lod{suffix}", mismatches[lod])


def _command_diagnostic(result: CommandResult) -> str:
    detail = result.stderr.strip() or result.stdout.strip() or "no diagnostic output"
    return f"exit {result.returncode}. {detail[-500:]}"


def _require_success(result: CommandResult, command: str, path: Path) -> None:
    if not result.success:
        raise RuntimeError(
            f"{command} failed for {path} ({_command_diagnostic(result)})"
        )


def _read_val3dity_report(
    runner: CommandRunner, path: Path, report_path: Path
) -> tuple[CommandResult, dict]:
    cmd = shlex.join(["{exe}", "--report", str(report_path), str(path)])
    result = runner.run(cmd, exe_name="val3dity", local_path=path.parent)
    try:
        with report_path.open() as stream:
            return result, json.load(stream)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"Could not read val3dity report {report_path} for {path} "
            f"({_command_diagnostic(result)})."
        ) from exc


def cityjson(
    system: CommandRunner,
    validation_runner: CommandRunner,
    dirpath: Path,
    file_id: str,
    url_root: str,
    version: str,
    specs: Specs3DBAGResource,
    url_file_id: str | None = None,
    *,
    tiles_dir_name: str = DEFAULT_TILES_DIR_NAME,
) -> CityJSONFileResults:
    """Validate a single CityJSON file.

    Args:
        system: CommandRunner for system tools
        validation_runner: CommandRunner for validation tools
        dirpath: Directory with the compressed cityjson file
        file_id: File name without extension
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
        cmd = shlex.join(["gunzip", "-t", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        results.zip_ok = bool(result.success and len(result.stdout) == 0)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Could not run gunzip -t for CityJSON archive %s. %s", inputzipfile, exc
        )
        inputfile.unlink(missing_ok=True)
        return results

    if not results.zip_ok:
        results.file_ok = False
        logger.error(
            "CityJSON archive %s failed the gunzip integrity check (%s). "
            "Check the compressed tile.",
            inputzipfile,
            _command_diagnostic(result),
        )
        return results

    # unzip
    try:
        cmd = shlex.join(["gunzip", "--keep", str(inputzipfile)])
        _require_success(system.run(cmd, cwd=str(dirpath)), "gunzip", inputzipfile)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Could not extract CityJSON archive %s to %s. %s",
            inputzipfile,
            inputfile,
            exc,
        )
        inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = shlex.join(["sha256sum", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        _require_success(result, "sha256sum", inputzipfile)
        sha256 = result.stdout.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root,
            format="cityjson",
            file_id=url_file_id or file_id,
            version=version,
            tiles_dir_name=tiles_dir_name,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Could not record the SHA-256 or download link for CityJSON archive %s. %s",
            inputzipfile,
            exc,
        )
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
        result, report = _read_val3dity_report(validation_runner, inputfile, reportfile)
        results.file_ok = result.success and report["validity"]
        _record_cityjson_report(results, report, cityobjects)
    except Exception as exc:
        inputfile.unlink(missing_ok=True)
        raise RuntimeError(
            f"Could not validate CityJSON geometry in {inputfile} with val3dity. {exc}"
        ) from exc
    finally:
        reportfile.unlink(missing_ok=True)
        logfile.unlink(missing_ok=True)

    # cjval
    try:
        cmd = shlex.join(["{exe}", str(inputfile), "-v"])
        result = validation_runner.run(cmd, exe_name="cjval", local_path=dirpath)
        pos = result.stdout.find("SUMMARY")
        summary = result.stdout[pos:]
        results.schema_valid = result.success and pos >= 0 and summary.find("valid") > 0
        results.schema_warnings = summary.find("warnings") > 0
        if pos < 0 or not result.success:
            logger.error(
                "cjval did not complete a successful schema check for %s (%s). "
                "Check the CityJSON file and the cjval executable.",
                inputfile,
                _command_diagnostic(result),
            )
    except Exception as exc:
        inputfile.unlink(missing_ok=True)
        raise RuntimeError(
            f"Could not run cjval on CityJSON tile {inputfile}."
        ) from exc

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
    url_root: str,
    version: str,
    archive_file_id: str | None = None,
    url_file_id: str | None = None,
    *,
    tiles_dir_name: str = DEFAULT_TILES_DIR_NAME,
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
        cmd = shlex.join(["unzip", "-t", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        entries_ok = result.stdout.count("OK")
        results.zip_ok = result.success and entries_ok == 6
        if not results.zip_ok:
            logger.error(
                "OBJ archive %s has %d of 6 expected entries passing unzip -t (%s). "
                "Check the three OBJ files and their materials.",
                inputzipfile,
                entries_ok,
                _command_diagnostic(result),
            )
    except Exception as exc:  # noqa: BLE001
        logger.error("Could not run unzip -t for OBJ archive %s. %s", inputzipfile, exc)
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = shlex.join(["sha256sum", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        _require_success(result, "sha256sum", inputzipfile)
        sha256 = result.stdout.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root,
            format="obj",
            file_id=url_file_id or file_id,
            version=version,
            tiles_dir_name=tiles_dir_name,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Could not record the SHA-256 or download link for OBJ archive %s. %s",
            inputzipfile,
            exc,
        )
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # unzip
    try:
        cmd = shlex.join(["unzip", "-o", str(inputzipfile)])
        _require_success(system.run(cmd, cwd=str(dirpath)), "unzip", inputzipfile)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Could not extract OBJ archive %s into %s. %s", inputzipfile, dirpath, exc
        )
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
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Could not read building and building part IDs from %s in archive %s. %s",
                    inputfile,
                    inputzipfile,
                    exc,
                )
                inputfile.unlink(missing_ok=True)
                return results
            try:
                result, report = _read_val3dity_report(
                    validation_runner, inputfile, reportfile
                )
                results.file_ok = (
                    results.file_ok is not False
                    and result.success
                    and report["validity"]
                )
                current_lod = re.search(r"(?<=LoD)\d{2}", inputfile.name).group(0)  # type: ignore[union-attr]
                # Each OBJ object is one report feature, including repeated building IDs.
                invalid_parts = [f for f in report["features"] if not f["validity"]]
                invalid_buildings = {f["id"].partition("-")[0] for f in invalid_parts}
                codes = {error["code"] for f in invalid_parts for error in f["errors"]}
                nr_invalid_building_all.append(len(invalid_buildings))
                setattr(
                    results,
                    f"nr_invalid_buildingpart_lod{current_lod}",
                    len(invalid_parts),
                )
                setattr(results, f"errors_lod{current_lod}", sorted(codes))
                reportfile.unlink()
                logfile.unlink(missing_ok=True)
            except Exception as exc:
                reportfile.unlink(missing_ok=True)
                logfile.unlink(missing_ok=True)
                inputfile.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Could not validate OBJ geometry in {inputfile} from {inputzipfile} "
                    f"with val3dity. {exc}"
                ) from exc
    if nr_building_all and nr_buildingpart_all and nr_invalid_building_all:
        results.nr_building = min(nr_building_all)
        results.nr_buildingpart = min(nr_buildingpart_all)
        results.nr_invalid_building = max(nr_invalid_building_all)
    else:
        results.file_ok = False
        logger.error(
            "OBJ archive %s produced no geometry counts. Check the LoD files and val3dity reports.",
            inputzipfile,
        )

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
    *,
    tiles_dir_name: str = DEFAULT_TILES_DIR_NAME,
) -> GPKGFileResults:
    results = GPKGFileResults()
    archive = dirpath / f"{file_id}.gpkg.gz"
    results.download = create_download_link(
        url_root, "gpkg", url_file_id or file_id, version, tiles_dir_name=tiles_dir_name
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
            logger.error(
                "GeoPackage archive %s failed validation. %s", archive, failure
            )
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
    *,
    tiles_dir_name: str = DEFAULT_TILES_DIR_NAME,
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
        cmd = shlex.join(["unzip", "-t", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        entries_ok = result.stdout.count("OK")
        results.zip_ok = result.success and entries_ok == 4
        if not results.zip_ok:
            logger.error(
                "IFC archive %s has %d of 4 expected entries passing unzip -t (%s). "
                "Check the IFC export for LoDs 0, 1.2, 1.3 and 2.2.",
                inputzipfile,
                entries_ok,
                _command_diagnostic(result),
            )
    except Exception as exc:  # noqa: BLE001
        logger.error("Could not run unzip -t for IFC archive %s. %s", inputzipfile, exc)
        return results

    # download link and sha256
    try:
        cmd = shlex.join(["sha256sum", str(inputzipfile)])
        result = system.run(cmd, cwd=str(dirpath))
        _require_success(result, "sha256sum", inputzipfile)
        results.sha256 = result.stdout.split(" ")[0]
        results.download = create_download_link(
            url_root=url_root,
            format="ifc",
            file_id=url_file_id or file_id,
            version=version,
            tiles_dir_name=tiles_dir_name,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Could not record the SHA-256 or download link for IFC archive %s. %s",
            inputzipfile,
            exc,
        )
        return results

    # number of IFC files in the archive
    try:
        with zipfile.ZipFile(inputzipfile) as zf:
            results.nr_ifc_files = len(zf.namelist())
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Could not list the files in IFC archive %s. %s", inputzipfile, exc
        )

    return results


def create_download_link(
    url_root: str,
    format: str,
    file_id: str,
    version: str,
    *,
    tiles_dir_name: str = DEFAULT_TILES_DIR_NAME,
) -> str:
    tile_id = file_id.replace("-", "/")
    version_stripped = version.replace(".", "")
    if format == "cityjson":
        filename = f"{file_id}.city.json.gz"
    elif format == "gpkg":
        filename = f"{file_id}.gpkg.gz"
    elif format == "obj":
        filename = f"{file_id}-obj.zip"
    elif format == "ifc":
        filename = f"{file_id}-ifc.zip"
    else:
        raise ValueError(
            f"Cannot build a download link for tile {file_id} in release {version} "
            f"with format {format!r}. Use cityjson, obj, gpkg, or ifc."
        )
    return f"{url_root}/{version_stripped}/{quote(tiles_dir_name, safe='')}/{tile_id}/{filename}"


def _check_formats(inputs) -> TileResults:
    """Worker function - no Dagster context available."""
    (
        validation_runner,
        gdal_runner,
        dirpath,
        tile_id,
        url_root,
        version,
        tiles_dir_name,
    ) = inputs

    # System tools runner (no configured exes needed)
    system = CommandRunner()

    specs = Specs3DBAGResource()
    file_id = tile_id.rsplit("/", 1)[-1]
    cj_results = cityjson(
        system=system,
        validation_runner=validation_runner,
        dirpath=dirpath,
        file_id=file_id,
        url_root=url_root,
        version=version,
        specs=specs,
        url_file_id=tile_id.replace("/", "-"),
        tiles_dir_name=tiles_dir_name,
    )
    obj_results = obj(
        system,
        validation_runner,
        dirpath,
        file_id,
        url_root=url_root,
        version=version,
        archive_file_id=tile_id.replace("/", "-"),
        url_file_id=tile_id.replace("/", "-"),
        tiles_dir_name=tiles_dir_name,
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
        tiles_dir_name=tiles_dir_name,
    )
    ifc_results = ifc(
        system,
        dirpath,
        file_id,
        url_root=url_root,
        version=version,
        archive_file_id=tile_id.replace("/", "-"),
        url_file_id=tile_id.replace("/", "-"),
        tiles_dir_name=tiles_dir_name,
    )
    return TileResults(tile_id, cj_results, obj_results, gpkg_results, ifc_results)


def check_formats(inputs) -> TileResults:
    try:
        return _check_formats(inputs)
    except Exception as exc:
        raise RuntimeError(
            f"Could not validate exported tile {inputs[3]} under {inputs[2]}. {exc}"
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
    tiles_dir: TilesDirResource,
) -> Output[Path]:
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
    path_tiles_dir = path_export_dir / tiles_dir.name
    url_root = "https://data.3dbag.nl"
    version_str = read_export_edition(metadata)
    validation_runner = validation.runner
    gdal_runner = gdal.runner
    tile_ids = read_indexed_tile_ids(export_index)
    inputs = (
        (
            validation_runner,
            gdal_runner,
            (path_tiles_dir / tile_id).parent,
            tile_id,
            url_root,
            version_str,
            tiles_dir.name,
        )
        for tile_id in tile_ids
    )
    logger.info(
        "Validating release %s using %d tiles under %s indexed in %s and 3D Tiles under %s",
        version_str,
        len(tile_ids),
        path_tiles_dir,
        export_index,
        path_export_dir,
    )
    if not tile_ids:
        logger.warning(
            "No compressed tiles to validate. Check the tile_id rows in %s.",
            export_index,
        )

    specs = Specs3DBAGResource()
    tileset_report = path_export_dir / "validate_3dtiles.csv"
    tileset_findings = {}
    with tileset_report.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["lod", "attribute", "outcome"])
        for lod, directory in (
            ("lod12", reconstruction_output_3dtiles_lod12),
            ("lod13", reconstruction_output_3dtiles_lod13),
            ("lod22", reconstruction_output_3dtiles_lod22),
        ):
            try:
                outcomes = set(inspect_tileset(directory, lod, specs))
            except Exception as exc:
                raise RuntimeError(
                    f"Could not inspect 3D Tiles metadata for {lod} under {directory}. {exc}"
                ) from exc
            tileset_findings[lod] = len(outcomes)
            if outcomes:
                logger.warning(
                    "%d 3D Tiles metadata findings for %s under %s. "
                    "See %s for the content files, attribute names, and outcomes.",
                    len(outcomes),
                    lod,
                    directory,
                    tileset_report,
                )
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
    failed_formats: dict[str, int] = {
        "CityJSON": 0,
        "OBJ": 0,
        "GeoPackage": 0,
        "IFC": 0,
    }
    first_failed = {}
    with output_path.open("w", encoding="utf-8", newline="") as stream:
        csvwriter = csv.DictWriter(
            stream, quoting=csv.QUOTE_NONNUMERIC, fieldnames=TileResults().fieldnames()
        )
        csvwriter.writeheader()
        with ProcessPoolExecutor(max_workers=config.concurrency) as executor:
            for result in executor.map(check_formats, inputs):
                csvwriter.writerow(result.asdict())
                for data_format, checks in (
                    ("CityJSON", (result.cityjson.zip_ok, result.cityjson.file_ok)),
                    ("OBJ", (result.obj.zip_ok, result.obj.file_ok)),
                    ("GeoPackage", (result.gpkg.zip_ok, result.gpkg.file_ok)),
                    ("IFC", (result.ifc.zip_ok, result.ifc.nr_ifc_files == 4)),
                ):
                    if not all(checks):
                        failed_formats[data_format] += 1
                        first_failed.setdefault(data_format, result.tile_id)
    for data_format, tile_id in first_failed.items():
        logger.warning(
            "%d of %d tiles did not pass %s archive or file checks (example tile %s). "
            "See %s for the tile IDs and check results.",
            failed_formats[data_format],
            len(tile_ids),
            data_format,
            tile_id,
            output_path,
        )
    return Output(
        output_path,
        metadata={
            "artifact path": MetadataValue.path(output_path),
            "3D Tiles report": MetadataValue.path(tileset_report),
            "export index": MetadataValue.path(export_index),
            "release": version_str,
            "tiles checked": len(tile_ids),
            "tiles failing checks by format": MetadataValue.json(failed_formats),
            "3D Tiles findings by LoD": MetadataValue.json(tileset_findings),
        },
    )
