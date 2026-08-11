#!/usr/bin/env bash
# Validate all .city.jsonl files in a directory using cjval.
# Usage: validate_cityjsonl.sh <input_dir>

set -euo pipefail

INPUT_DIR="${1:?Usage: $0 <input_dir>}"

mapfile -d '' FILES < <(find "$INPUT_DIR" -name "*.city.jsonl" -print0)

total=${#FILES[@]}
invalid=()

for f in "${FILES[@]}"; do
    log="${f%.city.jsonl}.cjval.log"
    cat "$f" | cjval > "$log" 2>&1 || true
    if grep -q $'❌' "$log"; then
        invalid+=("$f")
    fi
done

echo "Total .city.jsonl files: $total"
echo "Invalid: ${#invalid[@]}"

if [[ ${#invalid[@]} -gt 0 ]]; then
    echo ""
    echo "Invalid files:"
    for f in "${invalid[@]}"; do
        echo "  $f"
    done
fi
