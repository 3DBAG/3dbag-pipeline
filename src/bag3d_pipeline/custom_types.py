from pathlib import Path

from dagster import (PythonObjectDagsterType, make_python_type_usable_as_dagster_type)
from pgutils import PostgresTableIdentifier


LocalPath = PythonObjectDagsterType(
    python_type=Path, name="LocalPath",
    description="A path in the local filesystem."
)
make_python_type_usable_as_dagster_type(Path, LocalPath)


PostgresTable = PythonObjectDagsterType(
    python_type=PostgresTableIdentifier, name="PostgresTable",
    description="PostgreSQL database table.")
make_python_type_usable_as_dagster_type(python_type=PostgresTableIdentifier,
                                        dagster_type=PostgresTable)
