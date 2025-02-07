# bag3d-common – common resources, utilities and types for the 3D BAG pipeline

This package contains the resources, functions and type definitions that are used by the 3D BAG packages that define the data processing workflows.
Therefore, this package is meant to be a dependency of the workflow packages and does not contain data assets for instance.

## Install and use in dependent workflow package

Install this `bag3d-common` package manually in the virtual environment of the workflow package using its relative path:

```shell
uv add packages/common --editable
```

It will appear in the `pyproject.toml` file of the workflow package as:

```toml
[tool.uv.sources]
bag3d-common = { path = "packages/common", editable = true }
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
uv run mkdocs serve
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

## License

Licensed under Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any
additional terms or conditions.