from datetime import date

from dagster import TableColumn, TableSchema, OpExecutionContext, AssetKey
from dagster._core.definitions.data_version import extract_data_version_from_entry


def get_run_id(context, short=True):
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


def get_upstream_data_version(context: OpExecutionContext, asset_key: AssetKey) -> str:
    """Workaround for getting the upstream data version of an asset.
    Might change in future dagster.
    https://dagster.slack.com/archives/C01U954MEER/p1681931980941599?thread_ts=1681930694.932489&cid=C01U954MEER"""
    upstream_entry = context.get_step_execution_context().get_input_asset_record(
        asset_key).event_log_entry
    upstream_data_version = extract_data_version_from_entry(upstream_entry)
    return upstream_data_version.value
