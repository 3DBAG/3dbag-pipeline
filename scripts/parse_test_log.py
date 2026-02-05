"""Parse pytest output from test.log and extract unique warnings and errors.

Reads tests/test.log (or a custom log file) and extracts unique warnings and
errors, presenting them in an organized, readable format with occurrence counts
and file locations. Useful for identifying systematic issues across test runs.
"""

import argparse
import re
from collections import defaultdict
from pathlib import Path


def parse_warnings(log_content):
    """Extract warnings from pytest warning summary sections.

    Handles two pytest warning summary formats:
    1. Detailed format with full message lines
    2. Summary format with counts on location lines

    Args:
        log_content: Full log file content as string

    Returns:
        list of tuples: (warning_type, message, location, count)
    """
    warnings = defaultdict(lambda: {"count": 0, "location": "", "type": ""})

    # Find all warnings summary sections
    warnings_section_pattern = r"={10,} warnings summary ={10,}"

    for section_match in re.finditer(warnings_section_pattern, log_content):
        warnings_start = section_match.end()
        # Find end of this section (next separator or Docs line)
        separator_pattern = r"^-{2,} Docs:|^={10,}"
        rest = log_content[warnings_start:]
        separator_match = re.search(separator_pattern, rest, re.MULTILINE)

        if separator_match:
            warnings_end = warnings_start + separator_match.start()
        else:
            warnings_end = len(log_content)

        warnings_text = log_content[warnings_start:warnings_end]

        # Parse two formats:
        # 1. Location with count: "venv/...:70: 4 warnings"
        # 2. Detailed message: "  /opt/.../file.py:70: WarningType: message"

        lines = warnings_text.split("\n")
        i = 0
        while i < len(lines):
            line = lines[i]

            # Try format 1: location with count suffix
            # e.g., "venv/lib/python3.12/site-packages/dagster/_model/pydantic_compat_layer.py:70: 4 warnings"
            if (
                ": " in line
                and (" warning" in line.lower())
                and not line.startswith(" ")
                and not line.startswith("=")
                and not line.startswith("-")
            ):
                # Try to extract count from the line
                count_match = re.search(r"(\d+)\s+warning", line.lower())
                if count_match:
                    count = int(count_match.group(1))
                    # Look ahead for the detailed message
                    if i + 1 < len(lines):
                        next_line = lines[i + 1]
                        msg_match = re.match(
                            r"^\s+(/\S+\.py:\d+):\s+(\w+):\s+(.+)$", next_line
                        )
                        if msg_match:
                            actual_location = msg_match.group(1)
                            warning_type = msg_match.group(2)
                            message = msg_match.group(3)
                            key = (warning_type, message)
                            warnings[key]["count"] = count
                            warnings[key]["location"] = actual_location
                            warnings[key]["type"] = warning_type
                            i += 2
                            continue

            # Try format 2: just detailed message line
            # e.g., "  /opt/.../file.py:70: WarningType: message"
            msg_match = re.match(r"^\s+(/\S+\.py:\d+):\s+(\w+):\s+(.+)$", line)
            if msg_match:
                location = msg_match.group(1)
                warning_type = msg_match.group(2)
                message = msg_match.group(3)
                key = (warning_type, message)
                if key not in warnings:
                    warnings[key]["count"] = 1
                    warnings[key]["location"] = location
                    warnings[key]["type"] = warning_type
                else:
                    warnings[key]["count"] += 1

            i += 1

    result = []
    for (warning_type, message), data in warnings.items():
        result.append(
            (
                warning_type,
                message,
                data["location"],
                data["count"],
            )
        )

    return result


def format_color(text, color_code):
    """Apply ANSI color code to text."""
    return f"\033[{color_code}m{text}\033[0m"


def main():
    """Parse test log and display warnings/errors."""
    parser = argparse.ArgumentParser(
        description="Parse pytest log and extract unique warnings and errors"
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("tests/test.log"),
        help="Path to test log file (default: tests/test.log)",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colored output",
    )

    args = parser.parse_args()

    if not args.log_file.exists():
        print(f"Error: Log file not found: {args.log_file}")
        return 1

    with args.log_file.open() as f:
        log_content = f.read()

    if not log_content.strip():
        print("No warnings or errors found (empty log file)")
        return 0

    # Parse warnings
    warnings = parse_warnings(log_content)

    if not warnings:
        print("No warnings found in test log")
        return 0

    # Sort by count (descending)
    warnings_sorted = sorted(warnings, key=lambda x: x[3], reverse=True)

    # Format output
    use_color = not args.no_color

    if use_color:
        header = format_color(f"=== Test Log Analysis: {args.log_file} ===", "1")
        section_header = format_color("WARNINGS", "1;34")  # Bold blue
        count_fmt = lambda c: format_color(f"[{c} occurrence{'s' if c > 1 else ''}]", "1")  # Bold
        location_fmt = lambda l: format_color(l, "36")  # Cyan
    else:
        header = f"=== Test Log Analysis: {args.log_file} ==="
        section_header = "WARNINGS"
        count_fmt = lambda c: f"[{c} occurrence{'s' if c > 1 else ''}]"
        location_fmt = lambda l: l

    print(header)
    print()
    print(f"{section_header} ({len(warnings_sorted)} unique):")
    print("─" * 50)

    for warning_type, message, location, count in warnings_sorted:
        print(f"{count_fmt(count)} {warning_type}")
        print(f"  Location: {location_fmt(location)}")
        print(f"  Message: {message}")
        print()

    # Print summary
    summary = f"Summary: {len(warnings_sorted)} unique warnings"
    if use_color:
        summary = format_color(summary, "1")  # Bold
    print(summary)

    return 0


if __name__ == "__main__":
    exit(main())
