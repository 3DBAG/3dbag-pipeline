"""Custom types, custom Dagster types"""

from pathlib import Path
from dataclasses import dataclass
from typing import Sequence

from dagster import PythonObjectDagsterType, make_python_type_usable_as_dagster_type
from pgutils import PostgresTableIdentifier

LocalPath = PythonObjectDagsterType(
    python_type=Path, name="LocalPath", description="A path in the local filesystem."
)
make_python_type_usable_as_dagster_type(Path, LocalPath)

PostgresTable = PythonObjectDagsterType(
    python_type=PostgresTableIdentifier,
    name="PostgresTable",
    description="PostgreSQL database table.",
)
make_python_type_usable_as_dagster_type(
    python_type=PostgresTableIdentifier, dagster_type=PostgresTable
)


@dataclass
class ExportResult:
    """Result of the tile export with *tyler* for a single tile.

    Args:
        tile_id (str): Tile ID
        cityjson_path (Path): Path to the cityjson file
        gpkg_path (Path): Path to the geopackage file
        obj_paths (Sequence[Path]): Paths to the OBJ files
        wkt (str): Tile WKT
    """

    tile_id: str
    cityjson_path: Path
    gpkg_path: Path
    obj_paths: Sequence[Path]
    wkt: str

    @property
    def has_cityjson(self) -> bool:
        """Has an existing CityJSON file"""
        return self.cityjson_path is not None and self.cityjson_path.exists()

    @property
    def has_gpkg(self) -> bool:
        """Has an existing GeoPackage file"""
        return self.gpkg_path is not None and self.gpkg_path.exists()

    @property
    def has_obj(self) -> bool:
        """Has all the OBJ files and they exist"""
        return len(self.obj_paths) == 3 and all(p.exists() for p in self.obj_paths)

    def __iter__(self):
        for key in self.__dir__():
            if key[:2] != "__":
                yield key, getattr(self, key)
