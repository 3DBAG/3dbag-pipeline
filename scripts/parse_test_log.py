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


def parse_errors(log_content):
    """Extract errors from all pytest FAILURES sections.

    Processes all FAILURES sections in the log file to extract error information
    including error type and traceback details. Handles both standard exceptions
    (with colons) and assertion errors (without colons).

    Args:
        log_content: Full log file content as string

    Returns:
        list of tuples: (test_name, error_type, error_message)
    """
    errors = []

    # Find all FAILURES sections (there may be multiple from sequential test runs)
    failures_pattern = r"={10,} FAILURES ={10,}"

    for failures_match in re.finditer(failures_pattern, log_content):
        failures_start = failures_match.end()
        # Find end of failures section (next separator like "===")
        rest = log_content[failures_start:]
        end_pattern = r"^={10,}"
        end_match = re.search(end_pattern, rest, re.MULTILINE)

        if end_match:
            failures_end = failures_start + end_match.start()
        else:
            failures_end = len(log_content)

        failures_text = log_content[failures_start:failures_end]

        # Extract individual test failures
        # Pattern: "__ test_name __" or similar
        test_pattern = r"^_{2,}\s+(.+?)\s+_{2,}"
        lines = failures_text.split("\n")

        i = 0
        while i < len(lines):
            line = lines[i]
            test_match = re.match(test_pattern, line)

            if test_match:
                test_name = test_match.group(1)
                # Look for the error line with "E" prefix
                error_type = ""
                error_message = ""

                # Scan forward for error lines (starting with "E" followed by whitespace)
                # Keep searching until we find a line starting with "E", hit another test,
                # or reach end of lines
                j = i + 1
                while j < len(lines):
                    error_line = lines[j]

                    # Stop if we hit another test failure
                    if re.match(test_pattern, error_line):
                        break

                    # Check if this is an error line
                    if error_line.startswith("E ") or (error_line.startswith("E") and len(error_line) > 1 and error_line[1].isspace()):
                        # Extract error type and message
                        error_content = error_line.lstrip("E").strip()
                        # Format: "module.ErrorType: message" or "assert expression"
                        # Error types are typically after a dot and before the first colon

                        # Handle assertion errors (no colon)
                        if error_content.startswith("assert "):
                            error_type = "AssertionError"
                            error_message = error_content
                            break

                        # Handle standard exceptions (with colon)
                        elif ":" in error_content:
                            # Split on first colon to get error class and message
                            parts = error_content.split(":", 1)
                            error_type_full = parts[0].strip()
                            error_message = parts[1].strip() if len(parts) > 1 else ""

                            # Extract just the error type name (last part after dot)
                            if "." in error_type_full:
                                error_type = error_type_full.split(".")[-1]
                            else:
                                error_type = error_type_full

                            break

                        # Fallback for other error formats
                        elif error_content:
                            error_type = "UnknownError"
                            error_message = error_content
                            break

                    j += 1

                if error_type:
                    errors.append((test_name, error_type, error_message))

            i += 1

    return errors


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

    # Parse warnings and errors
    warnings = parse_warnings(log_content)
    errors = parse_errors(log_content)

    if not warnings and not errors:
        print("No warnings or errors found in test log")
        return 0

    # Format output
    use_color = not args.no_color

    if use_color:
        header = format_color(f"=== Test Log Analysis: {args.log_file} ===", "1")
        warnings_header = format_color("WARNINGS", "1;34")  # Bold blue
        errors_header = format_color("ERRORS", "1;31")  # Bold red
        count_fmt = lambda c: format_color(f"[{c} occurrence{'s' if c > 1 else ''}]", "1")  # Bold
        location_fmt = lambda l: format_color(l, "36")  # Cyan
        test_fmt = lambda t: format_color(t, "1;33")  # Bold yellow
    else:
        header = f"=== Test Log Analysis: {args.log_file} ==="
        warnings_header = "WARNINGS"
        errors_header = "ERRORS"
        count_fmt = lambda c: f"[{c} occurrence{'s' if c > 1 else ''}]"
        location_fmt = lambda l: l
        test_fmt = lambda t: t

    print(header)
    print()

    # Display errors first
    if errors:
        print(f"{errors_header} ({len(errors)}):")
        print("─" * 50)
        for test_name, error_type, error_message in errors:
            print(f"{test_fmt(test_name)}")
            print(f"  Error: {error_type}")
            if error_message:
                print(f"  Message: {error_message}")
            print()

    # Display warnings
    if warnings:
        warnings_sorted = sorted(warnings, key=lambda x: x[3], reverse=True)
        print(f"{warnings_header} ({len(warnings_sorted)} unique):")
        print("─" * 50)

        for warning_type, message, location, count in warnings_sorted:
            print(f"{count_fmt(count)} {warning_type}")
            print(f"  Location: {location_fmt(location)}")
            print(f"  Message: {message}")
            print()

    # Print summary
    summary_parts = []
    if errors:
        summary_parts.append(f"{len(errors)} error{'s' if len(errors) > 1 else ''}")
    if warnings:
        summary_parts.append(f"{len(warnings)} unique warning{'s' if len(warnings) > 1 else ''}")

    summary = f"Summary: {', '.join(summary_parts)}"
    if use_color:
        summary = format_color(summary, "1")  # Bold
    print(summary)

    return 0


if __name__ == "__main__":
    exit(main())
