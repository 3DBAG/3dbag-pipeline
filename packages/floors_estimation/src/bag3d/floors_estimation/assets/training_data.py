"""Assets for importing training data into the floors_estimation schema.

Loads floor count training datasets from Den Haag, Rotterdam, Amsterdam,
and Rijssen-Holten into PostgreSQL.
"""

from pathlib import Path

import pandas as pd
from dagster import (
    AssetKey,
    AutomationCondition,
    Config,
    Output,
    asset,
    get_dagster_logger,
)
from psycopg.sql import SQL, Identifier, Literal
from pydantic import Field

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import GDALResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import (
    create_schema,
    drop_table,
    postgrestable_metadata,
)
from bag3d.floors_estimation.resources import TrainingDataResource

SCHEMA = "floors_estimation"

logger = get_dagster_logger("floors_estimation.training_data")


class TrainingDataImportConfig(Config):
    """Configuration for training data file paths (relative to training data dir)."""

    denhaag_galerijflats: str = Field(
        default="training/denhaag/Levering galerijflats uniek pand zonder "
        "andere objecttypen aantal bouwlagen 10-9-2020.xlsx",
        description="Path to Den Haag gallery flats Excel file.",
    )
    denhaag_galerijflats_sheet: str = Field(
        default="Pand alleen galerijflat",
        description="Sheet name in the Den Haag gallery flats Excel file.",
    )
    denhaag_woonhuizen: str = Field(
        default="training/denhaag/Levering woonhuizen geheel perceel uniek "
        "pandid met aantal bouwlagen 10-9-2020.xlsx",
        description="Path to Den Haag houses Excel file.",
    )
    denhaag_woonhuizen_sheet: str = Field(
        default="EENGEZINSWONINGEN_GEH_PERCEEL",
        description="Sheet name in the Den Haag houses Excel file.",
    )
    rotterdam: str = Field(
        default="training/rotterdam/panden_sept_2020.csv",
        description="Path to Rotterdam training CSV file.",
    )
    amsterdam: str = Field(
        default="training/amsterdam/BAG_pand_Actueel.csv",
        description="Path to Amsterdam training CSV file.",
    )
    rijssen_holten: str = Field(
        default="RijssenHolten_NumberOfStoreys.geojson",
        description="Path to Rijssen-Holten GeoJSON file.",
    )


@asset(
    op_tags={"compute_kind": "python"},
    automation_condition=AutomationCondition.eager(),
    deps=[AssetKey(["floors_estimation", "import_training_denhaag_woonhuizen"])],
)
def import_training_denhaag_galerijflats(
    config: TrainingDataImportConfig,
    computation_db: DatabaseResource,
    training_data: TrainingDataResource,
) -> Output[PostgresTableIdentifier]:
    """Import Den Haag gallery flats floor count training data.

    Source: private data provided by Gemeente Den Haag.
    """
    table = PostgresTableIdentifier(SCHEMA, "training_denhaag_galerijflats")
    create_schema(computation_db, SCHEMA, logger=logger)
    drop_table(computation_db, table, logger=logger)

    path = Path(training_data.data_dir) / config.denhaag_galerijflats
    df = pd.read_excel(path, sheet_name=config.denhaag_galerijflats_sheet)
    logger.info(f"Loaded {len(df)} rows from {path}")

    df.to_sql(
        table.name,
        computation_db.connection.dsn,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
    )

    conn = computation_db.connection
    conn.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
            table.id, Identifier("PAND_ID")
        )
    )
    conn.send_query(
        SQL("COMMENT ON TABLE {} IS {}").format(
            table.id,
            Literal(
                "Number of floors for Gallery Apartment Buildings in Den Haag, "
                "private data provided by Gemeente Den Haag"
            ),
        )
    )

    metadata = postgrestable_metadata(computation_db, table)
    return Output(table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "python"},
    automation_condition=AutomationCondition.eager(),
)
def import_training_denhaag_woonhuizen(
    config: TrainingDataImportConfig,
    computation_db: DatabaseResource,
    training_data: TrainingDataResource,
) -> Output[PostgresTableIdentifier]:
    """Import Den Haag residential houses floor count training data.

    Source: private data provided by Gemeente Den Haag.
    """
    table = PostgresTableIdentifier(SCHEMA, "training_denhaag_woonhuizen")
    create_schema(computation_db, SCHEMA, logger=logger)
    drop_table(computation_db, table, logger=logger)

    path = Path(training_data.data_dir) / config.denhaag_woonhuizen
    df = pd.read_excel(path, sheet_name=config.denhaag_woonhuizen_sheet)
    logger.info(f"Loaded {len(df)} rows from {path}")

    df.to_sql(
        table.name,
        computation_db.connection.dsn,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
    )

    conn = computation_db.connection
    conn.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
            table.id, Identifier("PAND_ID")
        )
    )
    conn.send_query(
        SQL("COMMENT ON TABLE {} IS {}").format(
            table.id,
            Literal(
                "Number of floors for Residential Buildings in Den Haag, "
                "private data provided by Gemeente Den Haag"
            ),
        )
    )

    metadata = postgrestable_metadata(computation_db, table)
    return Output(table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "python"},
    automation_condition=AutomationCondition.eager(),
)
def import_training_rotterdam(
    config: TrainingDataImportConfig,
    computation_db: DatabaseResource,
    training_data: TrainingDataResource,
) -> Output[PostgresTableIdentifier]:
    """Import Rotterdam floor count training data.

    Source: private data provided by Gemeente Rotterdam.
    """
    table = PostgresTableIdentifier(SCHEMA, "training_rotterdam")
    create_schema(computation_db, SCHEMA, logger=logger)
    drop_table(computation_db, table, logger=logger)

    path = Path(training_data.data_dir) / config.rotterdam
    df = pd.read_csv(path, sep=";", dtype=object)
    logger.info(f"Loaded {len(df)} rows from {path}")

    df.drop_duplicates(subset=["PAND_ID"], inplace=True, ignore_index=True)
    logger.info(f"After dedup: {len(df)} rows")

    df.to_sql(
        table.name,
        computation_db.connection.dsn,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
    )

    conn = computation_db.connection
    conn.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
            table.id, Identifier("PAND_ID")
        )
    )
    conn.send_query(
        SQL("COMMENT ON TABLE {} IS {}").format(
            table.id,
            Literal(
                "Number of floors for buildings in Rotterdam, "
                "private data provided by Gemeente Rotterdam"
            ),
        )
    )

    metadata = postgrestable_metadata(computation_db, table)
    return Output(table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "python"},
    automation_condition=AutomationCondition.eager(),
)
def import_training_amsterdam(
    config: TrainingDataImportConfig,
    computation_db: DatabaseResource,
    training_data: TrainingDataResource,
) -> Output[PostgresTableIdentifier]:
    """Import Amsterdam floor count training data.

    Source: retrieved from ftp.data.amsterdam.nl.
    """
    table = PostgresTableIdentifier(SCHEMA, "training_amsterdam")
    create_schema(computation_db, SCHEMA, logger=logger)
    drop_table(computation_db, table, logger=logger)

    path = Path(training_data.data_dir) / config.amsterdam
    df = pd.read_csv(path, sep=";", dtype=object)
    logger.info(f"Loaded {len(df)} rows from {path}")

    df.dropna(subset=["aantalBouwlagen"], inplace=True)
    df.drop_duplicates(subset=["identificatie"], inplace=True, ignore_index=True)
    logger.info(f"After filtering: {len(df)} rows")

    df.to_sql(
        table.name,
        computation_db.connection.dsn,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
    )

    conn = computation_db.connection
    conn.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
            table.id, Identifier("identificatie")
        )
    )
    conn.send_query(
        SQL("COMMENT ON TABLE {} IS {}").format(
            table.id,
            Literal(
                "Number of floors for buildings in Amsterdam, "
                "retrieved from ftp.data.amsterdam.nl"
            ),
        )
    )

    metadata = postgrestable_metadata(computation_db, table)
    return Output(table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "ogr2ogr"},
    automation_condition=AutomationCondition.eager(),
)
def import_training_rijssen_holten(
    config: TrainingDataImportConfig,
    computation_db: DatabaseResource,
    gdal: GDALResource,
    training_data: TrainingDataResource,
) -> Output[PostgresTableIdentifier]:
    """Import Rijssen-Holten floor count training data from GeoJSON.

    Loads the GeoJSON via ogr2ogr into a staging table, then preprocesses
    using the training_table.sql query to produce the final training table.

    Source: provided by Gemeente Rijssen-Holten.
    """
    table = PostgresTableIdentifier(SCHEMA, "training_rijssen_holten")
    create_schema(computation_db, SCHEMA, logger=logger)
    drop_table(computation_db, table, logger=logger)

    path = Path(training_data.data_dir) / config.rijssen_holten

    cmd = " ".join(
        [
            "{exe}",
            "--config PG_USE_COPY=YES",
            "-overwrite",
            "-nln {new_table}",
            "-lco UNLOGGED=ON",
            "-lco GEOMETRY_NAME=geom",
            '-f PostgreSQL PG:"{dsn}"',
            '"{local_path}"',
        ]
    )

    result = gdal.runner.run(
        cmd,
        exe_name="ogr2ogr",
        kwargs={
            "new_table": table,
            "dsn": computation_db.connection.dsn,
        },
        local_path=path,
        logger=logger,
    )

    if not result.success:
        raise RuntimeError(
            f"ogr2ogr failed loading Rijssen-Holten GeoJSON: {result.stderr}"
        )

    conn = computation_db.connection
    conn.send_query(
        SQL("COMMENT ON TABLE {} IS {}").format(
            table.id,
            Literal(
                "Number of floors for buildings in Rijssen-Holten, "
                "provided by Gemeente Rijssen-Holten"
            ),
        )
    )

    metadata = postgrestable_metadata(computation_db, table)
    return Output(table, metadata=metadata)
