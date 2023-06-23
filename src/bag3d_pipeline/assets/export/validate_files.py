from bag3d_pipeline.resources.executables import execute_shell_command_silent
from pathlib import Path
import json
import re
import csv
from concurrent.futures import ThreadPoolExecutor


def cityjson(dirpath: Path, file_id: str, planarity_n_tol: float,
             planarity_d2p_tol: float) -> dict:
    results = {
        "cj_zip_ok": None,
        "cj_nr_features": None,
        "cj_nr_invalid": None,
        "cj_all_errors": None,
        "cj_schema_valid": None,
        "cj_schema_warnings": None
    }
    inputfile = dirpath / "7-80-368.city.json"

    # test zip
    try:
        cmd = " ".join([
            "gunzip", "-t", str(dirpath.joinpath(file_id).with_suffix(".city.json.gz"))
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        results["cj_zip_ok"] = True if len(output) == 0 else False
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

    # unzip
    try:
        cmd = " ".join([
            "gunzip", "--keep",
            str(dirpath.joinpath(file_id).with_suffix(".city.json.gz"))
        ])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

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
        cj_nr_features = report["features_overview"][0]["total"]
        cj_nr_invalid = report["features_overview"][0]["total"] - \
                        report["features_overview"][0]["valid"]
        results["cj_nr_features"] = cj_nr_features
        results["cj_nr_invalid"] = cj_nr_invalid
        results["cj_all_errors"] = report["all_errors"]
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
        results["cj_schema_valid"] = True if summary.find("valid") > 0 else False
        results["cj_schema_warnings"] = True if summary.find("warnings") > 0 else False
    except Exception:
        inputfile.unlink(missing_ok=True)
        return results

    # clean up
    inputfile.unlink()
    return results


def obj(dirpath: Path, file_id: str, planarity_n_tol: float,
        planarity_d2p_tol: float) -> dict:
    results = {
        "obj_zip_ok": None,
        "obj_nr_features": None,
        "obj_nr_invalid": None,
        "obj_all_errors": None,
    }
    inputfiles = [
        dirpath / f"{file_id}-LoD12-3D.obj", dirpath / f"{file_id}-LoD12-3D.obj.mtl",
        dirpath / f"{file_id}-LoD13-3D.obj", dirpath / f"{file_id}-LoD13-3D.obj.mtl",
        dirpath / f"{file_id}-LoD22-3D.obj", dirpath / f"{file_id}-LoD22-3D.obj.mtl"
    ]

    # test zip
    try:
        cmd = " ".join([
            "unzip", "-t", str(dirpath.joinpath(f"{file_id}-obj.zip"))
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        results["obj_zip_ok"] = True if output.count("OK") == 6 else False
    except Exception:
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # unzip
    try:
        cmd = " ".join([
            "unzip", "-o", str(dirpath.joinpath(f"{file_id}-obj.zip"))
        ])
        execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
    except Exception:
        for inputfile in inputfiles:
            inputfile.unlink(missing_ok=True)
        return results

    # val3dity
    reportfile = dirpath / "report.json"
    logfile = dirpath / "val3dity.log"
    nf = []
    ninvalid = []
    ae = []
    for inputfile in inputfiles:
        if inputfile.suffix == ".obj":
            try:
                cmd = " ".join(
                    ["/opt/bin/val3dity", "--planarity_n_tol", str(planarity_n_tol),
                     "--planarity_d2p_tol", str(planarity_d2p_tol),
                     "--report", str(reportfile), str(inputfile)])
                execute_shell_command_silent(shell_command=cmd, cwd=str(dirpath))
                with reportfile.open("r") as fo:
                    report = json.load(fo)
                nf.append(report["primitives_overview"][0]["total"])
                ninvalid.append(report["primitives_overview"][0]["total"] - \
                                report["primitives_overview"][0]["valid"])
                ae.extend(report["all_errors"])
                reportfile.unlink()
                logfile.unlink()
            except Exception:
                reportfile.unlink(missing_ok=True)
                logfile.unlink(missing_ok=True)
                inputfile.unlink(missing_ok=True)
                return results
    results["obj_nr_features"] = nf[0] if len(set(nf)) == 1 else -1
    results["obj_nr_invalid"] = ninvalid
    results["obj_all_errors"] = list(set(ae))

    for inputfile in inputfiles:
        inputfile.unlink()
    return results


def gpkg(dirpath: Path, file_id: str) -> dict:
    results = {
        "gpkg_zip_ok": None,
        "gpkg_ok": None,
        "gpkg_nr_features": None
    }
    inputzip = dirpath.joinpath(file_id).with_suffix(".gpkg.gz")

    # test zip
    try:
        cmd = " ".join([
            "gunzip", "-t", str(inputzip)
        ])
        output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                          cwd=str(dirpath))
        results["gpkg_zip_ok"] = True if len(output) == 0 else False
    except Exception:
        return results

    # ogrinfo
    nf = []
    for layer in ["lod12_3d", "lod13_3d", "lod22_3d"]:
        try:
            cmd = " ".join([
                "LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH",
                "/opt/bin/ogrinfo",
                f"/vsigzip/{inputzip}", layer
            ])
            output, returncode = execute_shell_command_silent(shell_command=cmd,
                                                              cwd=str(dirpath))
            results[
                "gpkg_ok"] = False if returncode != 0 or "error" in output.lower() else True
            try:
                n = int(re.search(r"(?<=Feature Count: )\d+", output).group(0))
            except Exception:
                n = 0
            nf.append(n)
        except Exception:
            return results
    results["gpkg_nr_features"] = nf[0] if len(set(nf)) == 1 else -1

    return results


def check_formats(input) -> dict:
    dirpath, tile_id = input
    file_id = tile_id.replace("/", "-")
    cj_results = cityjson(dirpath, file_id, planarity_n_tol=20.0,
                          planarity_d2p_tol=0.001)
    obj_results = obj(dirpath, file_id, planarity_n_tol=20.0, planarity_d2p_tol=0.001)
    gpkg_results = gpkg(dirpath, file_id)
    return {"tile": tile_id, **cj_results, **obj_results, **gpkg_results}


export_dir = Path("/data/3DBAG/export")
with open("/data/3DBAG/export/export_index.csv", "r") as fo:
    csvreader = csv.reader(fo)
    h = next(csvreader)
    tileids = [(export_dir.joinpath("tiles", row[0]), row[0]) for row in csvreader]

with ThreadPoolExecutor(max_workers=120) as executor:
    all_results = [result for result in executor.map(check_formats, tileids)]

with export_dir.joinpath("check_all_formats.csv").open("w") as fo:
    csvwriter = csv.DictWriter(
        fo, quoting=csv.QUOTE_NONNUMERIC,
        fieldnames=['tile', 'cj_zip_ok', 'cj_nr_features', 'cj_nr_invalid',
                    'cj_all_errors', 'cj_schema_valid', 'cj_schema_warnings',
                    'obj_zip_ok', 'obj_nr_features', 'obj_nr_invalid', 'obj_all_errors',
                    'gpkg_zip_ok', 'gpkg_ok', 'gpkg_nr_features'])
    csvwriter.writeheader()
    csvwriter.writerows(all_results)
