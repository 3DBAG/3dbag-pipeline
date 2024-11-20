# Documentation

Thank you for considering to contribute to the 3dbag-pipeline.
In this document we describe how can you get set up for writing documentation for the software.

## Setup

Clone the repository from [https://github.com/3DBAG/3dbag-pipeline](https://github.com/3DBAG/3dbag-pipeline).

The documentation is built with [mkdocs](https://www.mkdocs.org/) and several plugins.

We recommend you install the documentation dependencies in a virtual environment. You can do this with

```shell
make local_venv
```

Start the *mkdocs* server.

```shell
mkdocs serve
```

Verify that you can see the local documentation on `http://127.0.0.1:8000/` in your browser.

## Documenting the package

The APIs (eg. `common`) is documented with [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).
In order to generate the API documentation for a package, the package must be installed.
Solely for documentation purposes, this is best done with `pip install --no-deps packages/<package>`.
