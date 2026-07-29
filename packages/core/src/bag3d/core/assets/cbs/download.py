"""Download assets for CBS data sources.

Downloads CBS key figures (Kerncijfers wijken en buurten) from the OData API,
CBS Wijk- en buurtkaart (neighbourhood boundary geometries) as GeoPackage,
and CBS address-to-neighbourhood mapping as CSV.
"""

import re
from pathlib import Path

import requests
from dagster import (
    asset,
    Config,
    Output,
    get_dagster_logger,
    AutomationCondition,
)
from pydantic import Field

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.utils.files import unzip
from bag3d.common.utils.requests import download_file

logger = get_dagster_logger("cbs.download")


class CbsKeyFiguresConfig(Config):
    """Configuration for CBS key figures download."""

    year: str = Field(
        default="2025",
        description="Year of the key figures dataset.",
    )
    table_id: str = Field(
        default="86165NED",
        description="CBS OData table identifier. "
        "See https://opendata.cbs.nl for available tables.",
    )


class CbsBuurtkaartConfig(Config):
    """Configuration for CBS Wijk- en buurtkaart download."""

    year: str = Field(
        default="2025",
        description="Year of the Wijk- en buurtkaart.",
    )
    version: str = Field(
        default="v1",
        description="Version of the Wijk- en buurtkaart.",
    )


def _clean_cell(cell: object) -> str | int | float | None:
    """Remove special characters from CBS data cell contents.

    Strips whitespace, replaces cells containing only non-alphanumeric characters
    or 'None' with None (treated as NULL downstream). Preserves original
    numeric types from the OData JSON response.
    """
    if cell is None:
        return None
    cell_str = str(cell).strip()
    if re.match(r"^[_\W]+$", cell_str) or cell_str == "None":
        return None
    return cell if isinstance(cell, (int, float)) else cell_str


def _fetch_cbs_odata(target_url: str) -> list[dict]:
    """Fetch all records from OData API.

    Args:
        target_url: Base URL of the TypedDataSet endpoint
            (e.g. https://opendata.cbs.nl/ODataFeed/odata/85618NED/TypedDataSet).

    Returns:
        List of all records as dicts.
    """
    target_url = target_url + "?$format=json"
    records: list[dict] = []
    while target_url:
        response = requests.get(target_url, timeout=120)
        response.raise_for_status()
        payload = response.json()
        records.extend(payload.get("value", []))
        target_url = payload.get("@odata.nextLink") or payload.get("odata.nextLink")
    return records


def _clean_records(records: list[dict]) -> list[dict]:
    """Clean CBS OData records, returning dicts with proper Python types.

    The first 4 columns (ID + region identifiers) are kept as-is.
    Remaining columns have their values cleaned — special values ('---', 'None')
    become None, numeric values keep their Python type from the JSON response.
    """
    if not records:
        raise ValueError("No records to clean")

    cleaned: list[dict] = []
    for record in records:
        row: dict[str, object] = {}
        for i, (key, value) in enumerate(record.items()):
            if i < 4:
                # Keep ID and region identifier columns as-is
                row[key] = str(value).strip() if value is not None else None
            else:
                row[key] = _clean_cell(value)
        cleaned.append(row)
    return cleaned


@asset(automation_condition=AutomationCondition.eager())
def extract_cbs_key_figures(
    config: CbsKeyFiguresConfig,
) -> Output[list[dict]]:
    """Download CBS key figures (Kerncijfers wijken en buurten) from the OData API.

    Fetches data for the configured year and returns cleaned records with
    proper Python types preserved from the JSON response (int, float, str, None).

    API documentation: https://opendata.cbs.nl
    """
    api_url = f"https://opendata.cbs.nl/ODataFeed/odata/{config.table_id}/TypedDataSet"
    records = _fetch_cbs_odata(api_url)
    cleaned = _clean_records(records)

    metadata = {
        "Records": len(cleaned),
        "Year": config.year,
    }
    return Output(cleaned, metadata=metadata)


@asset(automation_condition=AutomationCondition.eager())
def extract_cbs_buurtkaart(
    config: CbsBuurtkaartConfig,
    file_store: FileStoreResource,
) -> Output[Path]:
    """Download the CBS Wijk- en buurtkaart GeoPackage.

    The Wijk- en buurtkaart contains the digital geometry of neighbourhood (buurt),
    district (wijk) and municipality (gemeente) boundaries in the Netherlands.

    Source: https://www.cbs.nl/nl-nl/dossier/nederland-regionaal/geografische-data
    """
    cbs_dir = file_store.create_subdir("cbs")
    zip_filename = f"WijkBuurtkaart_{config.year}_{config.version}.zip"
    url = f"https://geodata.cbs.nl/files/Wijkenbuurtkaart/{zip_filename}"

    zip_path = cbs_dir / zip_filename
    download_file(url, zip_path, chunk_size=1024 * 1024)
    unzip(zip_path, cbs_dir)

    # The ZIP extracts into a subdirectory with the GPKG inside
    gpkg_dir = cbs_dir / f"WijkBuurtkaart_{config.year}_{config.version}"
    gpkg_name = f"wijkenbuurten_{config.year}_{config.version}.gpkg"
    gpkg_path = gpkg_dir / gpkg_name

    if not gpkg_path.exists():
        # Fall back to searching for any GPKG in the extracted directory
        gpkg_files = list(gpkg_dir.glob("*.gpkg"))
        if not gpkg_files:
            raise FileNotFoundError(
                f"No GeoPackage found in {gpkg_dir}. Expected {gpkg_name}"
            )
        gpkg_path = gpkg_files[0]
        logger.warning(f"Expected GPKG {gpkg_name} not found, using {gpkg_path.name}")

    metadata = {
        "GeoPackage": str(gpkg_path),
        "Size [Mb]": round(gpkg_path.stat().st_size / 1e6, 2),
    }
    logger.debug(
        f"Downloaded CBS Wijk- en buurtkaart: {gpkg_path.name} ({metadata['Size [Mb]']} Mb)"
    )
    return Output(gpkg_path, metadata=metadata)
