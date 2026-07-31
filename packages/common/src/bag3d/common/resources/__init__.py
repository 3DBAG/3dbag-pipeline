import os
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

from dagster import get_dagster_logger

from bag3d.common.resources.cjindex import (
    CityIndexResource as CityIndexResource,
    open_ready_index as open_ready_index,
)
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import (
    GDALResource,
    PDALResource,
    LASToolsResource,
    TylerResource,
    RooferResource,
    ValidationResource,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.server_transfer import ServerTransferResource
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.version import ReleaseVersionResource, ToolVersionsResource
from bag3d.common.resources.values import NlTransform

# NOTE os.getenv() shows the env value in the Dagster UI, EnvVar hides the value in the Dagster UI
# Use os.environ[key] for required env vars: raises KeyError if unset and returns str (not
# str | None), which is both semantically correct and type-safe. Use os.getenv(key) only for
# genuinely optional env vars.

logger = get_dagster_logger()

if TYPE_CHECKING:
    from bag3d.common.resources.cjindex import CityIndexResource


class DagsterDeployment(StrEnum):
    DEFAULT = "default"
    PRODUCTION = "production"
    USER = "user"
    PYTEST = "pytest"
    PC = "pc"

    @classmethod
    def env_configured_deployments(cls) -> frozenset[str]:
        """Return deployment types that use environment variable configuration."""
        return frozenset({cls.PRODUCTION, cls.USER, cls.PYTEST, cls.PC})


version = ReleaseVersionResource(version=os.getenv("BAG3D_RELEASE_VERSION"))  # type: ignore[arg-type]

specs = Specs3DBAGResource()

nl_transform = NlTransform()

# Tool versions resource - instantiated at import time for code_version access
tool_versions = ToolVersionsResource(
    exe_tyler=os.getenv("EXE_PATH_TYLER"),
    exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"),
    exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"),
    exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
    exe_pdal=os.getenv("EXE_PATH_PDAL"),
    exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
    exe_geof=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
)


def resources_by_deployment(dagster_deployment: str) -> dict:
    configure_at_run_launch = False
    configure_from_env = False
    deployment_lower = dagster_deployment.lower()

    if deployment_lower == DagsterDeployment.DEFAULT:
        configure_at_run_launch = True
    elif deployment_lower in DagsterDeployment.env_configured_deployments():
        configure_from_env = True
    else:
        logger.warning(
            f"Invalid DAGSTER_DEPLOYMENT {dagster_deployment}, defaulting to 'default'"
        )
        configure_at_run_launch = True

    if configure_at_run_launch:
        return {
            "gdal": GDALResource.configure_at_launch(),
            "file_store": FileStoreResource.configure_at_launch(),
            "pointcloud_store": FileStoreResource.configure_at_launch(),
            "computation_db": DatabaseResource.configure_at_launch(),
            "pdal": PDALResource.configure_at_launch(),
            "lastools": LASToolsResource.configure_at_launch(),
            "tyler": TylerResource.configure_at_launch(),
            "validation": ValidationResource.configure_at_launch(),
            "roofer": RooferResource.configure_at_launch(),
            "version": version,
            "specs": specs,
            "publication_server": ServerTransferResource.configure_at_launch(),
            "publication_db": DatabaseResource.configure_at_launch(),
            "nl_transform": nl_transform,
            "reconstruction_index": CityIndexResource.configure_at_launch(),
            "party_walls_index": CityIndexResource.configure_at_launch(),
        }
    elif configure_from_env:
        return {
            "gdal": GDALResource(
                exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
                exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
                exe_sozip=os.getenv("EXE_PATH_SOZIP"),
            ),
            "file_store": FileStoreResource(root_dir=os.environ["BAG3D_FILESTORE"]),
            "pointcloud_store": FileStoreResource(
                root_dir=os.getenv(
                    "BAG3D_POINTCLOUD_DIR",
                    str(Path(os.environ["BAG3D_FILESTORE"]) / "pointcloud"),
                ),
            ),
            "computation_db": DatabaseResource(
                host=os.environ["BAG3D_PG_HOST"],
                user=os.environ["BAG3D_PG_USER"],
                password=os.environ["BAG3D_PG_PASSWORD"],
                port=int(os.environ["BAG3D_PG_PORT"]),
                dbname=os.environ["BAG3D_PG_DATABASE"],
                other_params={"sslmode": os.getenv("BAG3D_PG_SSLMODE", "allow")},
            ),
            "pdal": PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL")),
            "lastools": LASToolsResource(
                exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
                exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
                exe_lasinfo=os.getenv("EXE_PATH_LASINFO"),
            ),
            "tyler": TylerResource(
                exe_tyler=os.getenv("EXE_PATH_TYLER"),
                exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"),
            ),
            "validation": ValidationResource(
                exe_val3dity=os.getenv("EXE_PATH_VAL3DITY"),
                exe_cjval=os.getenv("EXE_PATH_CJVAL"),
                exe_cjio=os.getenv("EXE_PATH_CJIO"),
            ),
            "roofer": RooferResource(
                exe_crop=os.getenv("EXE_PATH_ROOFER_CROP"),
                exe_roofer=os.getenv("EXE_PATH_ROOFER_ROOFER"),
            ),
            "version": version,
            "specs": specs,
            "publication_server": ServerTransferResource(
                host=os.environ["BAG3D_PUBLICATION_HOST"],
                port=int(os.environ["BAG3D_PUBLICATION_PORT"])
                if os.environ.get("BAG3D_PUBLICATION_PORT")
                else None,
                user=os.environ["BAG3D_PUBLICATION_USER"],
                key_filename=os.getenv("BAG3D_PUBLICATION_KEY_FILENAME"),
                target_dir=os.environ["BAG3D_PUBLICATION_TARGET_DIR"],
                public_dir=os.getenv("BAG3D_PUBLICATION_PUBLIC_DIR"),
            ),
            "publication_db": DatabaseResource(
                host=os.environ["BAG3D_PUBLICATION_PG_HOST"],
                user=os.environ["BAG3D_PUBLICATION_PG_USER"],
                password=os.environ["BAG3D_PUBLICATION_PG_PASSWORD"],
                port=int(os.environ["BAG3D_PUBLICATION_PG_PORT"]),
                dbname=os.environ["BAG3D_PUBLICATION_PG_DATABASE"],
                other_params={
                    "sslmode": os.getenv("BAG3D_PUBLICATION_PG_SSLMODE", "allow")
                },
            ),
            "nl_transform": nl_transform,
            "reconstruction_index": CityIndexResource(
                dataset_dir=str(
                    Path(os.environ["BAG3D_FILESTORE"]) / "stages" / "reconstruction"
                )
            ),
            "party_walls_index": CityIndexResource(
                dataset_dir=str(
                    Path(os.environ["BAG3D_FILESTORE"]) / "stages" / "party_walls"
                )
            ),
        }
    else:
        raise RuntimeError("Cannot configure dagster environment")


def __getattr__(name: str):
    if name in {"CityIndexResource", "open_ready_index"}:
        from bag3d.common.resources.cjindex import CityIndexResource, open_ready_index

        globals().update(
            {
                "CityIndexResource": CityIndexResource,
                "open_ready_index": open_ready_index,
            }
        )
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


env_name = os.getenv("DAGSTER_DEPLOYMENT", "default")
resource_defs = resources_by_deployment(env_name)
