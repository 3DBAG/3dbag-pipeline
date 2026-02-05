Scripts for ad-hoc operations and analysis.

## Available Scripts

### Test Analysis

#### parse_test_log.py

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
make test_report       # Analyze warnings and errors from the test run
```

**Features:**

- Extracts both errors and warnings from test runs
- Aggregates warnings across multiple test sessions
- Deduplicates warnings by type and message content
- Sorts by frequency (most common warnings first)
- Shows test name and error type for failures
- Shows file location and full warning message for each unique warning
- Color-coded terminal output (disable with `--no-color`)
- Graceful handling of edge cases (missing files, empty logs, no warnings)

---

### Dagster Code Location Management

#### monitor-code-location.py

Checks if a Dagster code location has loaded successfully by querying the Dagster GraphQL API.

**Usage:**

```bash
# Check the default 'core' code location on localhost:3000
python3 scripts/monitor-code-location.py

# Check a specific code location
python3 scripts/monitor-code-location.py --code-location floors_estimation

# Check a remote Dagster instance
python3 scripts/monitor-code-location.py --host dagster.example.com --port 3000 --code-location core
```

**Options:**

- `--host` - Dagster host (default: `localhost`)
- `--port` - Dagster port (default: `3000`)
- `--code-location` - Code location name (default: `core`)

**Exit codes:**

- `0` - Code location loaded successfully
- `1` - Code location failed to load

**Use cases:**

- Health checks in monitoring/CI pipelines
- Verifying code location status after deployments
- Automated validation that all code locations are operational

#### reload-code-location.py

Reloads a Dagster code location without restarting the Dagster daemon. Useful for applying code changes without full system restarts.

**Usage:**

```bash
# Reload the default 'core' code location
python3 scripts/reload-code-location.py

# Reload a specific code location
python3 scripts/reload-code-location.py --code-location party_walls

# Reload on a remote Dagster instance
python3 scripts/reload-code-location.py --host dagster.example.com --port 3000 --code-location core
```

**Options:**

- `--host` - Dagster host (default: `localhost`)
- `--port` - Dagster port (default: `3000`)
- `--code-location` - Code location name to reload (default: `core`)

**Exit codes:**

- `0` - Reload succeeded
- `1` - Reload failed

**Note:** Requires the code location to support reloading (most do, see Dagster documentation for exceptions).

---

### Roofer Reconstruction Analysis

Tools for analyzing roofer reconstruction logs stored in Dagster.

#### roofer-logs-parse.py

Extracts reconstruction step timings from roofer logs recorded by Dagster. Parses `[reconstructor t]` timing records and exports them to CSV for analysis.

**Usage:**

```bash
# Extract timings from successful reconstructions
python3 scripts/roofer-logs-parse.py \
  --storage /opt/dagster/dagster_home/storage \
  --output reconstruction-times.csv

# Query from a remote Dagster instance
python3 scripts/roofer-logs-parse.py \
  --host dagster.example.com \
  --port 3000 \
  --storage /dagster/storage \
  --output results.csv
```

**Options:**

- `--host` - Dagster host (default: `localhost`)
- `--port` - Dagster port (default: `3000`)
- `--storage` - Path to Dagster storage location (required)
- `--output` / `-o` - Output CSV file path (required)

**Output CSV columns:**

The generated CSV includes timing data from roofer reconstruction runs, useful for performance analysis and optimization.

**Workflow:**

1. Queries Dagster for successful reconstruction runs
2. Retrieves stored logs from Dagster storage
3. Parses timing records from roofer logs
4. Exports results to CSV

#### roofer-logs-parse-debug.py

Analyzes unfinished buildings in reconstruction by parsing Dagster debug files. Helps identify buildings that failed to complete reconstruction.

**Usage:**

```bash
# Analyze debug files from a directory
python3 scripts/roofer-logs-parse-debug.py --debug-dir ./debug_files/
```

**Setup:**

1. Download debug files from Dagster UI:
   - Go to Runs overview
   - Click the dropdown menu of the 'View' button on a run
   - Select 'Download debug file'
   - Download all `.gz` files into a single directory

2. Run the script on that directory:

```bash
python3 scripts/roofer-logs-parse-debug.py --debug-dir /path/to/debug_files/
```

**Output:**

The script prints building IDs that have a `[reconstructor] start:` record but no matching `[reconstructor] finish:` record, indicating incomplete reconstructions.

**Use cases:**

- Debugging failed reconstruction runs
- Identifying which buildings need to be re-processed
- Performance analysis and troubleshooting

#### roofer-logs-plot.py

Generates matplotlib visualizations of reconstruction time statistics from `reconstruction-times.csv`. Creates bar charts showing median and mean reconstruction times.

**Requirements:**

```bash
pip install pandas matplotlib
```

**Usage:**

```bash
# Plot statistics (shows two plots: median and mean)
python3 scripts/roofer-logs-plot.py
```

**Output:**

- Two matplotlib windows showing:
  - Median reconstruction times by building
  - Mean reconstruction times by building
- CSV file `reconstruction-times-above_10min.csv` containing buildings that took >10 minutes

**Prerequisites:**

- Must have `scripts/reconstruction-times.csv` generated by `roofer-logs-parse.py`

---

### Database Utilities

#### bag_per_province.sql

SQL script that creates a view associating BAG (Building Address Group) data with Dutch provinces using the 2024 administrative boundaries.

**Setup:**

1. Download the latest administrative units GPKG from:
   - https://www.nationaalgeoregister.nl/geonetwork/srv/api/records/208bc283-7c66-4ce7-8ad3-1cf3e8933fb5
   - Example: https://service.pdok.nl/kadaster/bestuurlijkegebieden/atom/v1_0/downloads/BestuurlijkeGebieden_2024.gpkg

2. Load into PostgreSQL:

```bash
ogr2ogr -f PostgreSQL \
  -nln administrative_units.provinciegebied_2024 \
  PG:"dbname=baseregisters host=localhost port=5432 user=etl active_schema=administrative_units" \
  BestuurlijkeGebieden_2024.gpkg \
  provinciegebied
```

3. Execute the SQL script:

```bash
psql -d baseregisters -f scripts/bag_per_province.sql
```

**Output:**

- Creates `lvbag.pandactueelbestaand_provinces` view
- Each building is assigned to a province based on centroid containment
- Includes `provincienaam` column with province name

---

### File Organization

#### symlink_laz_per_province/

Creates directory structure per Dutch province and symlinks LAZ (LiDAR) files based on their geographic location using province boundaries and AHN tile index from webservices.

**Requirements:**

```bash
cd scripts/symlink_laz_per_province
pip install -r requirements.txt
```

**Usage:**

```bash
python3 scripts/symlink_laz_per_province/symlink_laz_per_province.py
```

**Configuration:**

Environment variables (from `.env`):
- Database connection details for querying province boundaries
- LAZ file locations

**Output:**

Creates symlinks organized by province:
```
output_dir/
  Drenthe/
    *.laz -> /original/path/file1.laz
  Friesland/
    *.laz -> /original/path/file2.laz
  ...
```

**Features:**

- Retrieves province boundaries from webservices
- Uses AHN tile index to locate LAZ files
- Creates directory per province
- Links files based on spatial containment

---

### Documentation Utilities

#### generate-changelog.txt

Instructions and template for generating a changelog from git commits and GitHub PRs.

**Process:**

1. Run the commands in this file to gather data:

```bash
git log 9e01ff013c4d3b858b44fc6b4cdb0026df865d86..HEAD --pretty=format:"%h%n%s%n%b%n----" > commits_full.txt

gh pr list --state all --limit 100 --json number,title,body,mergedAt,closedAt,createdAt \
  --jq ".[] | \"#\" + (.number|tostring) + \" \" + .title + \"\n\" + .body + \"\n----\"" > prs_full.txt
```

2. Use the generated files to create a condensed changelog focusing on:
   - Major PRs since the specified git commit
   - Terse descriptions
   - "Other updates" section for minor fixes, formatting, typo fixes, etc.

**Use cases:**

- Release notes generation
- Changelog updates
- Documentation of project evolution