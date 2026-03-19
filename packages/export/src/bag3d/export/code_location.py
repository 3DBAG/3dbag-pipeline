from dagster import Definitions

# Must be imported before any asset modules so that make_python_type_usable_as_dagster_type
# registers LocalPath for pathlib.Path before Dagster auto-registers it.
import bag3d.common.types  # noqa: F401

from bag3d.common.resources import resource_defs
from bag3d.export.asset_groups import all_assets
from bag3d.export.jobs import (
    job_export,
    job_deploy,
    job_release,
)

all_jobs = [
    job_export,
    job_deploy,
    job_release,
]

defs = Definitions(resources=resource_defs, assets=all_assets, jobs=all_jobs)
