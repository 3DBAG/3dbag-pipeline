import inspect
from importlib import resources
from logging import Logger

from dagster import get_dagster_logger, MarkdownMetadataValue
from psycopg.sql import SQL, Composed, Identifier, Literal
from pgutils import inject_parameters, PostgresTableIdentifier

from bag3d.common.resources.database import DatabaseResource


def load_sql(
    filename: str | None = None, query_params: dict | None = None
):  # pragma: no cover
    """Load SQL from a file and inject parameters if provided.

    If providing query parametes, they need to be in a dict, where the keys are the
    parameter names.

    The SQL script can contain parameters in the form of ``${...}``, which is
    understood by most database managers. This is handy for developing the SQL scripts
    in a database manager, without having to execute the pipeline.
    However, the python formatting only understands ``{...}`` placeholders, so the
    ``$`` are removed from ``${...}`` when the SQL is loaded from the file.

    Args:
        filename (str): SQL File to load (without the path) from the ``sql``
            sub-package. If None, it will load the ``.sql`` file with the name equal to
            the caller function's name.
        query_params (dict): If provided, the templated SQL is formatted with the
            parameters.

    For example:

    .. code-block:: python

        def my_func():
            load_sql() # loads my_func.sql from bag3d_pipeline.sql

    """
    # Find the name of the main package. This should be bag3d.<package>, e.g. bag3d.core
    stk = inspect.stack()[1]
    mod = inspect.getmodule(stk[0])
    if mod is None or mod.__package__ is None:
        raise RuntimeError("Could not determine calling module")
    pkgs = mod.__package__.split(".")
    if pkgs[0] != "bag3d" and len(pkgs) < 2:
        raise RuntimeError(
            "Trying to load SQL files from a namspace that is not bag3d.<package>."
        )
    sqlfiles_module = ".".join([pkgs[0], pkgs[1], "sqlfiles"])
    # Get the name of the calling function
    _f = filename if filename is not None else f"{inspect.stack()[1].function}.sql"
    _sql = resources.files(sqlfiles_module).joinpath(_f).read_text()
    _pysql = _sql.replace("${", "{")
    if query_params is None:
        return inject_parameters(_pysql)
    return inject_parameters(_pysql, query_params)


def summary_md(fields, null_count):
    logger = get_dagster_logger()
    if len(fields) != len(null_count):
        logger.error("fields and null_count are different length")
        return None
    metacols = ["column", "type", "NULLs"]
    header = " ".join(["|", " | ".join(metacols), "|"])
    header_separator = " ".join(["|", " | ".join("---" for _ in metacols), "|"])
    mdtbl = "\n".join([header, header_separator]) + "\n"
    _missing_vals = {rec["column_name"]: rec["missing_values"] for rec in null_count}
    for colname, coltype in fields:
        metarow = "| "
        metarow += f"**{colname}**" + " | "
        metarow += f"*{coltype}*" + " | "
        metarow += str(_missing_vals[colname]) + " |" + "\n"
        mdtbl += metarow
    # remove the last \n
    return mdtbl[:-1]


def postgrestable_from_query(
    computation_db: DatabaseResource,
    query: Composed,
    table: PostgresTableIdentifier,
    logger: Logger,
) -> dict:
    logger = logger or get_dagster_logger()
    conn = computation_db.connection
    # log the query
    logger.info(conn.print_query(query))
    # execute the query
    conn.send_query(query)
    return postgrestable_metadata(computation_db, table)


def postgrestable_metadata(
    computation_db: DatabaseResource, table: PostgresTableIdentifier
) -> dict:
    conn = computation_db.connection
    # row count
    row_count = conn.get_count(table)
    # schema
    fields = conn.get_fields(table)
    # null count
    null_count = conn.count_nulls(table)
    # head
    head = conn.get_head(table, md=True, shorten=23)
    return {
        "Database.Schema.Table": f"{conn.dbname}.{table}",
        "Rows": row_count,
        "Head": MarkdownMetadataValue(head if isinstance(head, str) else ""),
        "Summary": MarkdownMetadataValue(summary_md(fields, null_count)),
    }


def drop_table(computation_db: DatabaseResource, new_table, logger: Logger):
    """DROP TABLE IF EXISTS new_table CASCADE"""
    conn = computation_db.connection
    q = SQL("DROP TABLE IF EXISTS {tbl} CASCADE;").format(tbl=new_table.id)
    logger.info(conn.print_query(q))
    conn.send_query(q)


def create_schema(computation_db: DatabaseResource, new_schema: str, logger: Logger):
    """CREATE SCHEMA IF NOT EXISTS new_schema"""
    conn = computation_db.connection
    q = SQL("CREATE SCHEMA IF NOT EXISTS {sch};").format(sch=Identifier(new_schema))
    logger.info(conn.print_query(q))
    conn.send_query(q)


def table_exists(computation_db: DatabaseResource, table) -> bool:
    """CHECKS IF TABLE EXISTS"""
    query = SQL("""SELECT EXISTS (
                   SELECT FROM
                        pg_tables
                   WHERE
                        schemaname = {schema} AND
                        tablename  = {table}
                    );""").format(
        schema=Literal(table.schema.str), table=Literal(table.table.str)
    )
    res = computation_db.connection.get_dict(query)
    return res[0]["exists"]  # type: ignore[index]
