"""IFC conversion logic, refactored from IFC3DBAG's ``batch_converter.py``.

The original script was a ``click`` CLI that ran a multiprocessing pool over a
directory of ``.city.json.gz`` files. This module keeps only the single-tile
conversion logic, without the CLI, the multiprocessing pool, or the unzip step
(the pipeline feeds it already-uncompressed ``.city.json`` tiles and drives
concurrency from the Dagster asset).

Only the third-party imports below differ from the original: logging
replaces ``click.echo``.
"""

import gc
import logging
import os
import warnings
import zipfile
from pathlib import Path

from cjio import cityjson, errors

from .cityjson2ifc import Cityjson2ifc

logger = logging.getLogger(__name__)

# Which LoDs to export
LODS = ["0", "1.2", "1.3", "2.2"]


def load_cityjson(infile, ignore_duplicate_keys=False):
    """Load a CityJSON file and return a CityJSON model.

    Uses cjio's object-model API (``cityjson.reader`` + ``load_from_j``), with
    ``cityobjects`` populated and the raw ``CityObjects`` JSON member freed to
    save memory.
    """
    try:
        cm = cityjson.reader(file=infile, ignore_duplicate_keys=ignore_duplicate_keys)
    except ValueError as e:
        raise ValueError(f'{e}: "{infile.name}".') from e
    except OSError as e:
        raise OSError(f'Invalid file: "{infile.name}".\n{e}') from e

    # Check version and capture warnings
    try:
        with warnings.catch_warnings(record=True) as w:
            cm.check_version()
            for warn in w:
                logger.warning("CityJSON version warning: %s", warn.message)
    except errors.CJInvalidVersion as e:
        raise ValueError(e.msg) from e

    cm.cityobjects = {}
    cm.load_from_j(transform=False)
    cm.j["CityObjects"] = {}
    gc.collect()

    return cm


def convert_cityjson_to_ifc(
    cityjson_path: Path,
    ignore_duplicate_keys: bool = False,
    lods: list[str] | None = None,
) -> list[Path]:
    """Convert a single uncompressed ``.city.json`` tile into one IFC file per LoD.

    Args:
        cityjson_path: Path to the ``.city.json`` tile.
        ignore_duplicate_keys: Ignore duplicate JSON keys in the CityJSON file.
        lods: LoDs to export. Defaults to ``LODS``.

    Returns:
        The list of generated ``.ifc`` file paths.
    """
    lods = LODS if lods is None else lods
    output_ifc_files: list[Path] = []
    cm = None
    try:
        with open(cityjson_path, "r") as infile:
            logger.info("Parsing %s ...", infile.name)
            cm = load_cityjson(infile, ignore_duplicate_keys=ignore_duplicate_keys)
            for lod in lods:
                converter = Cityjson2ifc()
                output_ifc_path = Path(
                    str(cityjson_path).replace(".city.json", f"-{lod}.ifc")
                )
                converter.configuration(
                    name_project="3DBAG Project",
                    name_site="3DBAG Site",
                    name_person_family="3Dgeoinfo",
                    name_person_given="3DGI/",
                    lod=lod,
                    file_destination=str(output_ifc_path),
                )
                try:
                    converter.convert(cm)
                    output_ifc_files.append(output_ifc_path)
                except Exception as ex:  # noqa: BLE001
                    logger.warning(
                        "Failed to convert %s at LoD %s.\nError: %s",
                        cityjson_path,
                        lod,
                        ex,
                    )
                    continue
    except Exception as ex:  # noqa: BLE001
        logger.error("Error processing %s: %s", cityjson_path, ex)
    finally:
        if cm is not None:
            del cm
        gc.collect()
    return output_ifc_files


def zip_ifc_files(ifc_files: list[Path], zip_path: Path) -> Path:
    """Zip the given IFC files into a single archive and remove the originals."""
    zip_tmp = str(zip_path) + ".tmp"
    if os.path.isfile(zip_tmp):
        os.remove(zip_tmp)
    try:
        with zipfile.ZipFile(zip_tmp, "w") as zf:
            for ifc_file in ifc_files:
                zf.write(ifc_file, os.path.basename(ifc_file))
        os.rename(zip_tmp, zip_path)
    finally:
        for ifc_file in ifc_files:
            try:
                os.remove(ifc_file)
            except OSError:
                pass
    return zip_path


def convert_cityjson_to_ifc_zip(
    cityjson_path: Path,
    ignore_duplicate_keys: bool = False,
    lods: list[str] | None = None,
    zip_path: Path | None = None,
    force: bool = False,
) -> Path | None:
    """Convert a ``.city.json`` tile and zip the resulting IFC files.

    Args:
        cityjson_path: Path to the ``.city.json`` tile.
        ignore_duplicate_keys: Ignore duplicate JSON keys in the CityJSON file.
        lods: LoDs to export. Defaults to ``LODS``.
        zip_path: Destination ``.ifc.zip`` path. Defaults to the tile path with
            the ``.ifc.zip`` suffix.
        force: Reconvert even if the zip already exists.

    Returns:
        The path to the generated zip, or ``None`` if no IFC files were produced.
    """
    if zip_path is None:
        zip_path = Path(str(cityjson_path).replace(".city.json", ".ifc.zip"))
    if zip_path.is_file() and not force:
        logger.info("Zip file %s exists. Skipping %s.", zip_path, cityjson_path)
        return zip_path

    ifc_files = convert_cityjson_to_ifc(
        cityjson_path,
        ignore_duplicate_keys=ignore_duplicate_keys,
        lods=lods,
    )
    if not ifc_files:
        logger.warning("No IFC files generated for %s. Skipping zip.", cityjson_path)
        return None

    zip_ifc_files(ifc_files, zip_path)
    logger.info("Zipped IFC files into %s.", zip_path)
    return zip_path
