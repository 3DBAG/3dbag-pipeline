Scripts for ad-hoc operations and analysis.

## Available Scripts

### parse_test_log.py

Parses pytest output from `tests/test.log` and extracts unique warnings and errors, presenting them in an organized, readable format with occurrence counts and file locations.

**Usage:**

```bash
# Analyze test warnings from the default test.log file
python3 scripts/parse_test_log.py

# Disable colored output
python3 scripts/parse_test_log.py --no-color

# Analyze a custom log file
python3 scripts/parse_test_log.py --log-file /path/to/custom.log

# Get help
python3 scripts/parse_test_log.py --help
```

**Make Target:**

For convenience, you can also use the `make` target after running tests:

```bash
make test              # Run all tests and save to tests/test.log
make test_report       # Analyze warnings from the test run
```

**Features:**

- Aggregates warnings across multiple test sessions
- Deduplicates warnings by type and message content
- Sorts by frequency (most common warnings first)
- Shows file location and full warning message for each unique warning
- Color-coded terminal output (disable with `--no-color`)
- Graceful handling of edge cases (missing files, empty logs, no warnings)