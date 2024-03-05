from pathlib import Path
import json
import re
import csv
from concurrent.futures import ProcessPoolExecutor
import ast
from dataclasses import dataclass, field

from dagster import asset, AssetIn, AssetKey

from bag3d.common.resources.executables import execute_shell_command_silent
from bag3d.common.utils.files import bag3d_export_dir


@dataclass
class CityJSONFileResults:
    zip_ok: bool = None
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
    download: str = None
    sha256: str = None

    def asdict(self) -> dict:
        return {f"cj_{k}": v for k, v in self.__dict__.items()}


@dataclass
class OBJFileResults:
    zip_ok: bool = None
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
    zip_ok: bool = None
    file_ok: bool = None
    nr_building: int = None
    nr_buildingpart: int = None
    download: str = None
    sha256: str = None

    def asdict(self) -> dict:
        return {f"gpkg_{k}": v for k, v in self.__dict__.items()}


@dataclass
class TileResults:
    tile_id: str = None
    cityjson: CityJSONFileResults = field(default_factory=CityJSONFileResults)
    obj: OBJFileResults = field(default_factory=OBJFileResults)
    gpkg: GPKGFileResults = field(default_factory=GPKGFileResults)

    def fieldnames(self) -> list[str]:
        return ["tile_id", *self.cityjson.asdict().keys(), *self.obj.asdict().keys(),
                *self.gpkg.asdict().keys()]

    def asdict(self) -> dict:
        return {"tile_id": self.tile_id, **self.cityjson.asdict(), **self.obj.asdict(),
                **self.gpkg.asdict()}


def cityjson(dirpath: Path, file_id: str, planarity_n_tol: float,
             planarity_d2p_tol: float, url_root: str, version: str) -> CityJSONFileResults:
    results = CityJSONFileResults()
    inputzipfile = dirpath.joinpath(file_id).with_suffix(".city.json.gz")
    inputfile = dirpath / f"{file_id}.city.json"
    inputfile.unlink(missing_ok=True)  # in case a prev run failed

    # test zip
    try:
        cmd = " ".join([
            "gunzip", "-t", str(inputzipfile)
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        results.zip_ok = True if len(output) == 0 else False
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

    # unzip
    try:
        cmd = " ".join([
            "gunzip", "--keep",
            str(inputzipfile)
        ])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = " ".join([
            "sha256sum", str(inputfile)
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        sha256 = output.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root, format="cityjson", file_id=file_id, version=version
        )
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

    # cjio feature and lod count
    try:
        cmd = " ".join(
            ["/home/bdukai/software/3dbag-pipeline/venvs/venv_core/bin/cjio", str(inputfile),
             "info", "--long"])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        try:
            results.nr_building = int(re.search(r"(?<=Building \()\d+", output).group(0))
        except Exception:
            results.nr_building = -1
        try:
            results.nr_buildingpart = int(re.search(r"(?<=BuildingPart \()\d+", output).group(0))
        except Exception:
            results.nr_buildingpart = -1
        try:
            results.lod = ast.literal_eval(re.search(r"(?<=LoD = ).+", output).group(0))
        except Exception:
            results.lod = ""
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

    # Read the whole CityJSON again, so that we can match the val3dity errors to the
    # errors in the b3_val3dity attributes. It would be better to combine this with the
    # object and lod count above.
    with inputfile.open("r") as fo:
        cm = json.load(fo)
        cityobjects = cm["CityObjects"]

    # val3dity
    reportfile = dirpath / "report.json"
    logfile = dirpath / "val3dity.log"
    try:
        cmd = " ".join(
            ["/opt/bin/val3dity", "--planarity_n_tol", str(planarity_n_tol),
             "--planarity_d2p_tol", str(planarity_d2p_tol),
             "--report", str(reportfile), str(inputfile)])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
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
                nr_invalid_lod12 += 0 if feature["primitives"][lod12_idx][
                    "validity"] else 1
                nr_invalid_lod13 += 0 if feature["primitives"][lod13_idx][
                    "validity"] else 1
                nr_invalid_lod22 += 0 if feature["primitives"][lod22_idx][
                    "validity"] else 1
                e12 = set(e["code"] for e in feature["primitives"][lod12_idx]["errors"])
                e13 = set(e["code"] for e in feature["primitives"][lod13_idx]["errors"])
                e22 = set(e["code"] for e in feature["primitives"][lod22_idx]["errors"])
                errors_lod12.update(e12)
                errors_lod13.update(e13)
                errors_lod22.update(e22)
                cj_co = cityobjects.get(feature["id"])
                if cj_co:
                    if e12 != set(eval(cj_co["attributes"]["b3_val3dity_lod12"])):
                        nr_mismatch_errors_lod12 += 1
                    if e13 != set(eval(cj_co["attributes"]["b3_val3dity_lod13"])):
                        nr_mismatch_errors_lod13 += 1
                    if e22 != set(eval(cj_co["attributes"]["b3_val3dity_lod22"])):
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
        reportfile.unlink()
        logfile.unlink()
    except Exception:
        reportfile.unlink(missing_ok=True)
        logfile.unlink(missing_ok=True)
        inputfile.unlink(missing_ok=True)
        return results

    # cjval
    try:
        cmd = " ".join(["/opt/bin/cjval", str(inputfile)])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        pos = output.find("SUMMARY")
        summary = output[pos:]
        results.schema_valid = True if summary.find("valid") > 0 else False
        results.schema_warnings = True if summary.find("warnings") > 0 else False
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

    # clean up
    inputfile.unlink()
    return results


def obj(dirpath: Path, file_id: str, planarity_n_tol: float, planarity_d2p_tol: float,
        url_root: str, version: str) -> OBJFileResults:
    results = OBJFileResults()
    inputzipfile = dirpath.joinpath(f"{file_id}-obj.zip")
    inputfiles = [
        dirpath / f"{file_id}-LoD12-3D.obj", dirpath / f"{file_id}-LoD12-3D.obj.mtl",
        dirpath / f"{file_id}-LoD13-3D.obj", dirpath / f"{file_id}-LoD13-3D.obj.mtl",
        dirpath / f"{file_id}-LoD22-3D.obj", dirpath / f"{file_id}-LoD22-3D.obj.mtl"
    ]
    for inputfile in inputfiles:
        inputfile.unlink(missing_ok=True)

    # test zip
    try:
        cmd = " ".join([
            "unzip", "-t", str(inputzipfile)
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        results.zip_ok = True if output.count("OK") == 6 else False
    except Exception:
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = " ".join([
            "sha256sum", str(inputzipfile)
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        sha256 = output.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root, format="obj", file_id=file_id, version=version
        )
    except Exception:
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # unzip
    try:
        cmd = " ".join([
            "unzip", "-o", str(inputzipfile)
        ])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
    except Exception:
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # val3dity
    reportfile = dirpath / "report.json"
    logfile = dirpath / "val3dity.log"
    nr_building_all = []
    nr_buildingpart_all = []
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
                        bpid_match = re.search(r"(?<=o )NL\.IMBAG\.Pand\.\d{16}-\d+",
                                               line)
                        if bid_match:
                            building_ids.add(bid_match.group(0))
                            buildingpart_ids_temp_until_obj_fix.append(
                                bid_match)  # this can be removed after the OBJ fix
                        elif bpid_match:
                            buildingpart_ids.add(bpid_match.group(0))
                nr_building_all.append(len(building_ids))
                nr_buildingpart_all.append(len(buildingpart_ids_temp_until_obj_fix))
            except Exception:
                inputfile.unlink(missing_ok=True)
                return results
            try:
                cmd = " ".join(
                    ["/opt/bin/val3dity", "--planarity_n_tol", str(planarity_n_tol),
                     "--planarity_d2p_tol", str(planarity_d2p_tol),
                     "--report", str(reportfile), str(inputfile)])
                execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
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
                results.nr_invalid_building = len(invalid_building_ids)
                results.nr_invalid_buildingpart_lod12 = nr_invalid_lod12
                results.nr_invalid_buildingpart_lod13 = nr_invalid_lod13
                results.nr_invalid_buildingpart_lod22 = nr_invalid_lod22
                results.errors_lod12 = list(errors_lod12)
                results.errors_lod13 = list(errors_lod13)
                results.errors_lod22 = list(errors_lod22)
                reportfile.unlink()
                logfile.unlink()
            except Exception:
                reportfile.unlink(missing_ok=True)
                logfile.unlink(missing_ok=True)
                inputfile.unlink(missing_ok=True)
                return results

    for inputfile in inputfiles:
        inputfile.unlink()
    return results


def gpkg(dirpath: Path, file_id: str, url_root: str, version: str) -> GPKGFileResults:
    results = {
        "gpkg_zip_ok": None,
        "gpkg_ok": None,
        "gpkg_nr_features": None,
        "gpkg_sha256": None,
        "gpkg_download": None
    }
    results = GPKGFileResults()
    inputzipfile = dirpath.joinpath(file_id).with_suffix(".gpkg.gz")
    inputfile = dirpath.joinpath(file_id).with_suffix(".gpkg")
    propertiesfile = dirpath.joinpath(file_id).with_suffix(".gpkg.gz.properties")

    # test zip
    try:
        cmd = " ".join([
            "gunzip", "-t", str(inputzipfile)
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        results.zip_ok = True if len(output) == 0 else False
    except Exception:
        return results

    # unzip
    try:
        cmd = " ".join([
            "gunzip", "--keep",
            str(inputzipfile)
        ])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

    # download link and sha256
    try:
        cmd = " ".join([
            "sha256sum", str(inputfile)
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        sha256 = output.split(" ")[0]
        results.sha256 = sha256
        results.download = create_download_link(
            url_root=url_root, format="gpkg", file_id=file_id, version=version
        )
    except Exception:
        return results
    finally:
        inputfile.unlink(missing_ok=True)

    # ogrinfo
    nr_building_all = []
    nr_buildingpart_all = []
    for layer in ["lod12_3d", "lod13_3d", "lod22_3d"]:
        try:
            sql_buildingpart_count = f"-sql 'select count(identificatie) from {layer}'"
            sql_building_count = f"-sql 'select count(distinct identificatie) from {layer}'"

            cmd = " ".join([
                "LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH",
                "/opt/bin/ogrinfo",
                sql_buildingpart_count,
                f"/vsigzip/{inputzipfile}"
            ])
            output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                              cwd=str(dirpath))
            results.file_ok = False if returncode != 0 or "error" in output.lower() else True
            re_buildingpart_count = r"(?<=count\(identificatie\) \(Integer\) = )\d+"
            try:
                n = int(re.search(re_buildingpart_count, output).group(0))
            except Exception:
                n = None
            nr_buildingpart_all.append(n)

            cmd = " ".join([
                "LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH",
                "/opt/bin/ogrinfo",
                sql_building_count,
                f"/vsigzip/{inputzipfile}"
            ])
            output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                              cwd=str(dirpath))
            re_building_count = r"(?<=count\(distinct identificatie\) \(Integer\) = )\d+"
            try:
                n = int(re.search(re_building_count, output).group(0))
            except Exception:
                n = None
            nr_building_all.append(n)
        except Exception:
            return results
    results.nr_building = min(nr_building_all)
    results.nr_buildingpart = min(nr_buildingpart_all)
    propertiesfile.unlink(missing_ok=True)
    return results


def create_download_link(url_root: str, format: str, file_id: str, version: str) -> str:
    tile_id = file_id.replace("-", "/")
    version_stripped = version.replace(".", "")
    if format == "cityjson":
        filename = f"{file_id}.city.json"
        l = f"{url_root}/{format}/{version_stripped}/tiles/{tile_id}/{filename}"
    elif format == "gpkg":
        filename = f"{file_id}.gpkg"
        l = f"{url_root}/{format}/{version_stripped}/tiles/{tile_id}/{filename}"
    elif format == "obj":
        filename = f"{file_id}-obj.zip"
        l = f"{url_root}/{format}/{version_stripped}/tiles/{tile_id}/{filename}"
    else:
        raise ValueError(f"only cityjson, obj, gpkg format is allowed, got {format}")
    return l


def check_formats(input) -> TileResults:
    dirpath, tile_id, url_root, version = input
    file_id = tile_id.replace("/", "-")
    planarity_n_tol = 20.0
    planarity_d2p_tol = 0.001
    cj_results = cityjson(dirpath, file_id, planarity_n_tol=planarity_n_tol,
                          planarity_d2p_tol=planarity_d2p_tol, url_root=url_root,
                          version=version)
    obj_results = obj(dirpath, file_id, planarity_n_tol=planarity_n_tol,
                      planarity_d2p_tol=planarity_d2p_tol,
                      url_root=url_root, version=version)
    gpkg_results = gpkg(dirpath, file_id, url_root=url_root, version=version)
    return TileResults(tile_id, cj_results, obj_results, gpkg_results)


@asset(
    ins={
        "export_index": AssetIn(key_prefix="export"),
        "metadata": AssetIn(key_prefix="export"),
    },
    deps=[
        AssetKey(("export", "compressed_tiles"))
    ],
    required_resource_keys={"file_store"}
)
def compressed_tiles_validation(context, export_index: Path, metadata: Path) -> Path:
    """Validates the compressed distribution tiles, for each format.
    Save the validation results to a CSV.
    Validation is done concurrently per tile.

    Validation:

    - check if the archive is valid
    - compute the SHA-256 of the archive
    - add the download links per format
    - number of features per format
    - run val3dity on the CityJSON and OBJ formats and record the number of invalids
        and the error codes
    - CityJSON schema validation
    - CityJSON LoD-s present in the file
    """
    path_export_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    url_root = "https://data.3dbag.nl"
    with metadata.open("r") as fo:
        metadata_json = json.load(fo)
        version = metadata_json["identificationInfo"]["citation"]["edition"]
    with export_index.open("r") as fo:
        csvreader = csv.reader(fo)
        h = next(csvreader)
        tileids = [(path_export_dir.joinpath("tiles", row[0]), row[0], url_root, version)
                   for row in csvreader]

    output_path = path_export_dir.joinpath("validate_compressed_files.csv")
    fo = output_path.open("w")
    csvwriter = csv.DictWriter(
        fo, quoting=csv.QUOTE_NONNUMERIC,
        fieldnames=TileResults().fieldnames())
    csvwriter.writeheader()

    try:
        with ProcessPoolExecutor() as executor:
            for result in executor.map(check_formats, tileids):
                csvwriter.writerow(result.asdict())
    finally:
        fo.close()

    return output_path