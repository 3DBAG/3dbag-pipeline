import re
import sys
from typing import Optional


def parse_changelog(version: str) -> Optional[str]:
    """Parse CHANGELOG.md and return the content for the specified version.

    Args:
        version: Version string to find in the changelog (e.g. "2024.12.16")

    Returns:
        String containing the changelog entry for the specified version,
        or None if version not found
    """
    with open("CHANGELOG.md", "r") as f:
        content = f.read()

    # Pattern to match version headers
    version_header_pattern = r"## \[([\d.]+)\]"

    # Find all version headers and their positions
    version_matches = list(re.finditer(version_header_pattern, content))

    # Find the index of our target version
    target_index = None
    for i, match in enumerate(version_matches):
        if match.group(1) == version:
            target_index = i
            break

    if target_index is None:
        print(f"No changelog entry found for version {version}")
        return None

    # Get the start position (right after the version header)
    start_pos = version_matches[target_index].end()

    # Get the end position (start of next version or end of file)
    if target_index + 1 < len(version_matches):
        end_pos = version_matches[target_index + 1].start()
    else:
        end_pos = len(content)

    # Extract the content between these positions
    changelog_content = content[start_pos:end_pos]

    # Clean up the content
    # Remove leading/trailing whitespace and empty lines while preserving internal formatting
    cleaned_lines = []
    for line in changelog_content.split('\n'):
        if line.strip() or cleaned_lines:  # Keep empty lines only after we've started collecting content
            cleaned_lines.append(line)

    # Remove trailing empty lines
    while cleaned_lines and not cleaned_lines[-1].strip():
        cleaned_lines.pop()

    return '\n'.join(cleaned_lines)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parse_changelog.py VERSION")
        sys.exit(1)

    version = sys.argv[1]
    changelog_content = parse_changelog(version)

    if changelog_content:
        with open("RELEASE_NOTES.md", "w") as f:
            f.write(changelog_content)
        print("Successfully extracted changelog content")
    else:
        print("Failed to extract changelog content")
        sys.exit(1)
