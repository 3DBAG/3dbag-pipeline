# Documentation

Thank you for considering to contribute to the 3dbag-pipeline.
In this document we describe how can you get set up for writing documentation for the software, how to submit a contribution, what guidelines do we follow and what do we expect from your contribution, what can you expect from us.

## Setup

Clone the repository from [https://github.com/3DBAG/3dbag-pipeline](https://github.com/3DBAG/3dbag-pipeline).

The documentation is built with [mkdocs](https://www.mkdocs.org/) and several plugins.
First, install the documentation dependencies (into a virtual environment).

```shell
pip install -r requirements_docs.txt
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
