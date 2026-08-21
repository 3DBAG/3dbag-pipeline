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
import warnings
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
