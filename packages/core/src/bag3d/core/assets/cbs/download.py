"""Download assets for CBS data sources.

Downloads CBS key figures (Kerncijfers wijken en buurten) from the OData API,
CBS Wijk- en buurtkaart (neighbourhood boundary geometries) as GeoPackage,
and CBS address-to-neighbourhood mapping as CSV.
"""

import csv
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

    table_ids: dict[str, str] = Field(
        default={"2025": "86165NED", "2024": "85984NED"},
        description="Mapping of year to CBS OData table identifier. "
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


class CbsAddressMappingConfig(Config):
    """Configuration for CBS address-to-neighbourhood mapping download."""

    year: str = Field(
        default="2023",
        description="Year for naming the output table.",
    )
    url: str = Field(
        default="https://www.cbs.nl/-/media/_excel/2023/35/2023-cbs-pc6huisnr20230801_buurt.zip",
        description="Full download URL for the CBS address mapping ZIP.",
    )


def _clean_cell(cell: object) -> str:
    """Remove special characters from CBS data cell contents.

    Strips whitespace, replaces cells containing only non-alphanumeric characters
    or 'None' with an empty string (to be treated as NULL/NaN downstream).
    """
    cell = str(cell).strip()
    if re.match(r"^[_\W]+$", cell) or cell == "None":
        return ""
    return cell


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


def _records_to_csv(records: list[dict], output_path: Path) -> None:
    """Write a list of dicts to a CSV file, cleaning cell values.

    The first 4 columns (ID + region identifiers) are kept as-is.
    Remaining columns have their values cleaned and empty strings written
    for special/missing values.
    """
    if not records:
        raise ValueError("No records to write")

    fieldnames = list(records[0].keys())

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for record in records:
            cleaned = {}
            for i, (key, value) in enumerate(record.items()):
                if i < 4:
                    # Keep ID and region identifier columns as-is
                    cleaned[key] = str(value).strip() if value is not None else ""
                else:
                    cleaned[key] = _clean_cell(value)
            writer.writerow(cleaned)


@asset(automation_condition=AutomationCondition.eager())
def extract_cbs_key_figures(
    config: CbsKeyFiguresConfig,
    file_store: FileStoreResource,
) -> Output[dict[str, Path]]:
    """Download CBS key figures (Kerncijfers wijken en buurten) from the OData API.

    Fetches data for each configured year and saves as CSV files in the file store.
    The CBS OData API provides neighbourhood-level statistics including population
    density, housing types, and distances to amenities.

    API documentation: https://opendata.cbs.nl
    """
    cbs_dir = file_store.create_subdir("cbs")
    result: dict[str, Path] = {}
    metadata: dict = {}

    for year, table_id in config.table_ids.items():
        api_url = f"https://opendata.cbs.nl/ODataFeed/odata/{table_id}/TypedDataSet"
        records = _fetch_cbs_odata(api_url)

        csv_path = cbs_dir / f"cbs_key_figures_{year}.csv"
        _records_to_csv(records, csv_path)

        result[year] = csv_path
        metadata[f"Records [{year}]"] = len(records)
        metadata[f"File [{year}]"] = str(csv_path)

    return Output(result, metadata=metadata)


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
    logger.debug(f"Downloaded CBS Wijk- en buurtkaart: {gpkg_path.name} ({metadata['Size [Mb]']} Mb)")
    return Output(gpkg_path, metadata=metadata)


@asset(automation_condition=AutomationCondition.eager())
def extract_cbs_address_mapping(
    config: CbsAddressMappingConfig,
    file_store: FileStoreResource,
) -> Output[Path]:
    """Download the CBS postcode-to-neighbourhood mapping.

    This ZIP contains a CSV mapping each postcode + house number combination
    to its corresponding gemeente, wijk, and buurt codes.

    Source: https://www.cbs.nl/nl-nl/maatwerk/2023/35/buurt-wijk-en-gemeente-2023-voor-postcode-huisnummer
    Source: https://www.cbs.nl/nl-nl/maatwerk/2024/35/buurt-wijk-en-gemeente-2024-voor-postcode-huisnummer
    Source: https://www.cbs.nl/nl-nl/maatwerk/2025/38/buurt-wijk-en-gemeente-2025-voor-postcode-huisnummer
    """
    extract_dir = file_store.create_subdir(f"cbs/address_mapping_{config.year}")
    zip_path = extract_dir / f"cbs_address_mapping_{config.year}.zip"

    download_file(config.url, zip_path, chunk_size=1024 * 1024)
    
    unzip(zip_path, extract_dir)

    # Find the extracted CSV in the year-specific directory
    csv_files = list(extract_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV file found in {extract_dir} after extracting {zip_path.name}"
        )
    csv_path = csv_files[0]
    logger.info(f"Extracted address mapping CSV: {csv_path.name}")

    metadata = {
        "CSV": str(csv_path),
        "Year": config.year,
    }
    return Output(csv_path, metadata=metadata)
