import os
from enum import StrEnum

from dagster import get_dagster_logger

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import (
    GDALResource,
    PDALResource,
    LASToolsResource,
    TylerResource,
    RooferResource,
    GeoflowResource,
    ValidationResource,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.server_transfer import ServerTransferResource
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.version import ReleaseVersionResource, ToolVersionsResource

# NOTE os.getenv() shows the env value in the Dagster UI, EnvVar hides the value in the Dagster UI
# Use os.environ[key] for required env vars: raises KeyError if unset and returns str (not
# str | None), which is both semantically correct and type-safe. Use os.getenv(key) only for
# genuinely optional env vars.

logger = get_dagster_logger()


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

# Tool versions resource - instantiated at import time for code_version access
tool_versions = ToolVersionsResource(
    exe_tyler=os.getenv("EXE_PATH_TYLER"),
    exe_tyler_db=os.getenv("EXE_PATH_TYLER_DB"),
    exe_tyler_multiformat=os.getenv("EXE_PATH_TYLER_MULTIFORMAT"),
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
            "file_store_fastssd": FileStoreResource.configure_at_launch(),
            "production_db": DatabaseResource.configure_at_launch(),
            "pdal": PDALResource.configure_at_launch(),
            "lastools": LASToolsResource.configure_at_launch(),
            "tyler": TylerResource.configure_at_launch(),
            "geoflow": GeoflowResource.configure_at_launch(),
            "validation": ValidationResource.configure_at_launch(),
            "roofer": RooferResource.configure_at_launch(),
            "version": version,
            "specs": specs,
            "publication_server": ServerTransferResource.configure_at_launch(),
            "publication_db": DatabaseResource.configure_at_launch(),
        }
    elif configure_from_env:
        return {
            "gdal": GDALResource(
                exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
                exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
                exe_sozip=os.getenv("EXE_PATH_SOZIP"),
            ),
            "file_store": FileStoreResource(data_dir=os.environ["BAG3D_FILESTORE"]),
            "file_store_fastssd": FileStoreResource(
                data_dir=os.environ["BAG3D_FILESTORE_FASTSSD"]
            ),
            "production_db": DatabaseResource(
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
                exe_tyler_multiformat=os.getenv("EXE_PATH_TYLER_MULTIFORMAT"),
            ),
            "geoflow": GeoflowResource(
                exe_geoflow=os.getenv("EXE_PATH_ROOFER_RECONSTRUCT"),
                flowchart=os.getenv("FLOWCHART_PATH_RECONSTRUCT"),
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
        }
    else:
        raise RuntimeError("Cannot configure dagster environment")


env_name = os.getenv("DAGSTER_DEPLOYMENT", "default")
resource_defs = resources_by_deployment(env_name)
