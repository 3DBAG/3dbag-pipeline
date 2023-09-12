# 3D BAG pipeline

Repository of the 3D BAG production pipeline.

## Packages

The packages are organized into a `common` package and a number of workflow packages.
The `common` package contains the resources, functions and type definitions that are used by the 3D BAG packages that define the data processing workflows.
The workflow packages contain the assets, jobs, sensors etc. that define a data processing workflow for a part of the complete 3D BAG.

- `common`: The common package used by the workflow packages.
- `core`: Workflow for producing the core of the 3D BAG data set.
- `party_walls`: Workflow for calculating the party walls.

## Documentation

The `common` package contains API documentation that can be viewed locally.
The documentation of the components of the workflow packages can be viewed in the Dagster UI.

## Development and testing

You need to have the workflow packages set up in their own virtual environments in `/venvs`.
The virtual environment names follow the pattern of `venv_<package>`, e.g. `/venvs/venv_core`, `/venvs/venv_party_walls`.

Create another one for the `dagster-webserver` and install the package with `pip install dagster-webserver`.

```shell
cd 3dbag-pipeline/tests/dagster_home
dagster dev
```

### Conventions

SQL files are stored in the `sqlfiles` subpackage, so that the `bag3d.common.utils.database.load_sql` function can load them.
