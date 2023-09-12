# bag3d-common – common resources, utilities and types for the 3D BAG pipeline

This package contains the resources, functions and type definitions that are used by the 3D BAG packages that define the data processing workflows.
Therefore, this package is meant to be a dependency of the workflow packages and does not contain data assets for instance.

## Install and use in dependent workflow package

Install this `bag3d-common` package manually in the virtual environment of the workflow package:

```shell
pip install 3dbag-pipeline/packages/common
```

`pyproject.toml` of the workflow package:

```toml
dependencies = [
    "bag3d-common",
]
```

For example use the resources in some `module.py` in the workflow package:

```python
from bag3d.common.resources import database

database.db_connection
```

## Documentation

The API is documented with [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).
The documentation is built with [mkdocs](https://www.mkdocs.org/).

Install the documentation dependencies and view the docs locally:

```shell
cd 3dbag-pipeline/packages/common
pip install -e ".[docs]"
mkdocs serve
```

Go to `http://127.0.0.1:8000/` in your browser.

### Commands

* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs -h` - Print help message and exit.

### Documentation layout

    mkdocs.yml    # The configuration file.
    docs/
        index.md  # The documentation homepage.
        ...       # Other markdown pages, images and other files.

## Development and testing

Install for development with the dev dependencies into its own virtual environment.

```shell
cd 3dbag-pipeline/packages/common
pip install -e ".[dev]"
```

Test with [tox](https://tox.wiki/en/latest/).

```shell
tox
```

But you can also run individual tests in `/tests` with `pytest`.
