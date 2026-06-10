import requests
import csv
import re
from pathlib import Path


def _clean_cell(cell: object) -> str:
    """Remove special characters from CBS data cell contents.

    Strips whitespace, replaces cells containing only non-alphanumeric characters
    or 'None' with an empty string (to be treated as NULL/NaN downstream).
    """
    cell = str(cell).strip()
    if re.match(r"^[_\W]+$", cell) or cell == "None":
        return ""
    return cell


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


def get_odata(target_url: str) -> list[dict]:
    """Fetch all records from OData API using nextLink pagination.

    This implementation avoids pandas and returns a list of dict records.
    """
    records: list[dict] = []
    while target_url:
        response = requests.get(target_url, timeout=120)
        response.raise_for_status()
        print(f"Fetched {len(records)} records so far...")
        payload = response.json()
        records.extend(payload.get("value", []))

        target_url = payload.get("@odata.nextLink") or payload.get("odata.nextLink")

    return records


api_url = "https://opendata.cbs.nl/ODataFeed/odata/85984NED/TypedDataSet?$format=json"

records = get_odata(api_url)

csv_path = Path("cbs_key_figures_2024.csv")
_records_to_csv(records, csv_path)
