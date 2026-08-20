"""Utilities for working with the dagster instance"""

from datetime import date

from dagster import (
    AssetExecutionContext,
    StaticPartitionsDefinition,
    TableColumn,
    TableSchema,
    get_dagster_logger,
)

from bag3d.common.utils.files import get_export_tile_ids


def get_run_id(context: AssetExecutionContext, short=True):
    """Return the Run ID from the execution context.

    Args:
        context: A dagster context object.
        short (bool): Return only the first 8 characters of the ID or the complete ID.
    """
    if context is None:
        return None
    if context.dagster_run is None:
        return None
    if short:
        return context.dagster_run.run_id.split("-")[0]
    else:
        return context.dagster_run.run_id.split("-")


def cast_to_dagsterschema(fields: list):
    columns = [TableColumn(name=colname, type=coltype) for colname, coltype in fields]
    return TableSchema(columns=columns)


def format_date(input_date: date, version: bool = True) -> str:
    """Formats a date for using it in versions, filenames, attributes etc.

    Args:
        input_date:
        version: If True, format the input_date as '9999.99.99', else as '9999-99-99'
    """
    if version:
        return input_date.strftime("%Y.%m.%d")
    else:
        return input_date.strftime("%Y-%m-%d")


class PartitionDefinition3DBagDistribution(StaticPartitionsDefinition):
    """Distribution tiles"""

    def __init__(self):
        logger = get_dagster_logger("PartitionDefinition3DBagDistribution")
        try:
            tile_ids = get_export_tile_ids()
        except BaseException:
            logger.exception("Failed to get export tile IDs")
            tile_ids = []
        super().__init__(partition_keys=sorted(tile_ids))
