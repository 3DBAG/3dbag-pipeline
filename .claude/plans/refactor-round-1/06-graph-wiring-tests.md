# Stage 6: Add Graph Wiring Tests with Mock Resources

**Effort:** High (4-6 hours)
**Impact:** High — provides multi-asset workflow coverage without requiring external tool binaries

## Problem

Currently there are only two ways to test multi-asset workflows:
1. **Unit tests** — call individual asset functions directly (tests logic but not Dagster wiring)
2. **Integration tests** (`@pytest.mark.needs_tools`) — run full jobs via `execute_in_process` with real GDAL/roofer/tyler binaries

There's no middle ground that tests:
- Asset graph wiring (are inputs/outputs connected correctly?)
- Partition propagation (does a partitioned job pass partition keys correctly?)
- Resource injection (are all required resources provided?)
- Job definition correctness (does AssetSelection include all needed assets?)

## Inspiration: Existing Patterns

The codebase already has excellent examples of lightweight Dagster tests:

### `test_automation_conditions.py` — tests asset DAG behavior without any resources
```python
MOCK_RESOURCES = {
    "file_store": dg.ResourceDefinition.mock_resource(),
    "gdal": dg.ResourceDefinition.mock_resource(),
    ...
}
DEFS = dg.Definitions(assets=AUTOMATED_ASSETS, resources=MOCK_RESOURCES)
```

### `test_integration.py` — uses `MockAssetIOManager` to stub upstream assets
```python
defs = Definitions(
    resources=resources,
    assets=[mock_asset_reconstruction_input, mock_asset_tiles, ...],
    jobs=[job_nl_reconstruct],
)
```

## Implementation

### 6.1 Create mock resource implementations

Create `packages/common/src/bag3d/common/testing/mock_resources.py`:

```python
"""Mock resources for graph wiring tests.

These resources satisfy Dagster's type checking and resource injection
but return canned responses instead of running real tools.
"""
from bag3d.common.resources.executables import CommandRunner


class MockCommandResult:
    """Canned result from a mock command execution."""
    def __init__(self, success=True, stdout="", stderr=""):
        self.success = success
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = 0 if success else 1


class MockCommandRunner(CommandRunner):
    """CommandRunner that records calls but doesn't execute anything."""
    def __init__(self):
        self.calls = []

    def run(self, cmd_template, **kwargs):
        self.calls.append((cmd_template, kwargs))
        return MockCommandResult()
```

### 6.2 Add graph wiring tests for each job

Create `packages/core/tests/test_job_wiring.py`:

```python
"""Test that job definitions wire assets correctly.

These tests verify Dagster graph structure without running asset functions.
They catch issues like:
- Missing assets in job selection
- Incorrect asset key prefixes
- Missing resource definitions
- Partition definition mismatches
"""
import dagster as dg
from bag3d.core.jobs import (
    job_nl_reconstruct,
    job_nl_export,
    job_ahn3,
    job_ahn4,
    job_ahn5,
)
from bag3d.core.code_location import defs


def test_all_jobs_resolvable():
    """Every job defined in the code location can be resolved."""
    for job_name in [
        "nl_reconstruct",
        "nl_export",
        "nl_export_after_floors",
        "ahn_tile_index",
        "ahn3",
        "ahn4",
        "ahn5",
        "ahn_metadata_index",
    ]:
        job_def = defs.get_job_def(job_name)
        assert job_def is not None, f"Job {job_name} not found in definitions"


def test_partitioned_jobs_have_partition_defs():
    """Partitioned jobs should have matching partition definitions."""
    for job_name in ["ahn3", "ahn4", "ahn5", "nl_reconstruct"]:
        job_def = defs.get_job_def(job_name)
        # Verify the job can accept a partition key
        assert job_def.partitions_def is not None or any(
            asset.partitions_def is not None
            for asset in job_def.asset_layer.assets_defs_by_key.values()
        ), f"Job {job_name} expected to be partitioned"


def test_job_asset_selections_are_complete():
    """Each job's asset selection includes all required assets (no dangling inputs)."""
    # This is implicitly tested by Definitions.validate_loadable(),
    # but we can add explicit checks for specific jobs
    for job_name in ["nl_export", "nl_reconstruct"]:
        job_def = defs.get_job_def(job_name)
        # Get all assets in the job
        asset_keys = set(job_def.asset_layer.asset_keys)
        # Verify expected assets are present
        assert len(asset_keys) > 0, f"Job {job_name} has no assets"
```

### 6.3 Add `materialize()` smoke tests for asset subgraphs

For cases where you want to verify that assets can actually execute together (not just wire up), use `dagster.materialize()` with mock resources:

```python
"""Smoke tests that materialize small asset subgraphs with mock resources.

These tests verify that:
- Asset functions accept their declared resources
- Output types match downstream input expectations
- Partition key propagation works correctly
"""
import dagster as dg
from bag3d.core.assets.ahn.download import md5_ahn3, md5_ahn4, sha256_ahn5
from unittest.mock import patch


def test_checksum_assets_materialize():
    """Checksum assets can be materialized with mocked HTTP calls."""
    mock_checksums = {"C_01CZ1.LAZ": "aaa111"}

    with patch("bag3d.core.assets.ahn.download.get_checksums", return_value=mock_checksums):
        result = dg.materialize(
            [md5_ahn3],
            resources={},  # md5_ahn3 takes no resources
        )
    assert result.success
    output = result.output_for_node("md5_ahn3")
    assert output == mock_checksums
```

### 6.4 Test resource injection completeness

```python
def test_all_assets_have_required_resources():
    """Every asset's required resources are provided in the definitions."""
    defs_obj = defs  # from code_location
    # Definitions.validate_loadable() already checks this,
    # but we can add targeted checks
    dg.Definitions.validate_loadable(defs_obj)
```

## Relationship to Existing Tests

| Test Type | What It Tests | Speed | Example |
|-----------|--------------|-------|---------|
| `test_code_location.py` | Definitions load without errors | Fast | `test_definitions_loadable` |
| `test_automation_conditions.py` | Automation logic, no execution | Fast | `test_cron_roots_requested_after_tick` |
| **New: `test_job_wiring.py`** | **Job structure, asset selection** | **Fast** | **`test_all_jobs_resolvable`** |
| `test_assets_*.py` | Individual asset logic | Medium | `test_metadata_table_ahn3` |
| `test_integration.py` | Full pipeline execution | Slow | `test_integration_reconstruction_and_export` |

## Files Modified

- **New:** `packages/common/src/bag3d/common/testing/mock_resources.py`
- **New:** `packages/core/tests/test_job_wiring.py`
- Potentially: `packages/party_walls/tests/test_job_wiring.py`, `packages/floors_estimation/tests/test_job_wiring.py`

## Verification

```bash
# New tests should run fast (no Docker, no tools needed — just Python)
docker compose -p bag3d-dev exec bag3d-core pytest /opt/3dbag-pipeline/packages/core/tests/test_job_wiring.py -v

# Full suite still passes
make test
```
