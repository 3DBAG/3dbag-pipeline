"""Generate a Markdown coverage summary table from per-package coverage JSON files."""

import json
import sys
from pathlib import Path


def main():
    coverage_files = sorted(Path(".").glob(".coverage.*.json"))
    if not coverage_files:
        print("No coverage JSON files found.", file=sys.stderr)
        sys.exit(1)

    rows = []
    for path in coverage_files:
        # Extract package name from .coverage.<pkg>.json
        pkg = path.stem.split(".", 1)[1]
        data = json.loads(path.read_text())
        totals = data["totals"]
        stmts = totals["num_statements"]
        miss = totals["missing_lines"]
        cover = round(totals["percent_covered"])
        rows.append((pkg, stmts, miss, cover))

    print("## Test Coverage")
    print()
    print("| Package | Stmts | Miss | Cover |")
    print("|---------|-------|------|-------|")
    for pkg, stmts, miss, cover in rows:
        print(f"| {pkg} | {stmts} | {miss} | {cover}% |")


if __name__ == "__main__":
    main()
