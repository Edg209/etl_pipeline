import psycopg
import pandas as pd
import logging
from database.exceptions import MissingPrimaryKeyException

logger = logging.getLogger('Database')


def load_dataframe_to_table(schema_name: str, table_name: str, df: pd.DataFrame, cur: psycopg.cursor.Cursor):
    """Load a dataframe into a table where the dataframe header is contained within the table header."""
    # If the dataframe has no rows, we exit early
    if len(df) == 0:
        logger.debug("No rows to load, skipping load process")
        return
    columns = ','.join(df.columns)
    logger.debug(f"Loading {len(df)} row(s) into {schema_name}.{table_name} ({columns})")
    if schema_name is not None:
        insert_query = f"copy {schema_name}.{table_name} ({columns}) from STDIN"
    else:
        insert_query = f"copy {table_name} ({columns}) from STDIN"
    with cur.copy(insert_query) as cp:
        for rec in df.convert_dtypes().to_records(index=False):
            cp.write_row(rec)


def get_primary_key_name(schema_name: str, table_name: str, cur: psycopg.cursor.Cursor) -> str:
    """Retrieve the name of the primary key of a table."""
    # We use an arbitrary aggregation of max to ensure that we always return exactly one row
    # If the count returns a value other than 1, we will raise an exception
    select_sql = "select count(*), max(constraint_name) from information_schema.table_constraints where constraint_type = 'PRIMARY KEY' and table_schema = %s and table_name = %s"
    cur.execute(select_sql, (schema_name, table_name))
    sql_results = cur.fetchone()
    if sql_results[0] != 1:
        raise MissingPrimaryKeyException
    return sql_results[1]


def get_key_columns(schema_name: str, key_name: str, cur: psycopg.cursor.Cursor) -> list[str]:
    """Retrieve a list of columns used in a key in the correct order."""
    select_sql = "select column_name from information_schema.key_column_usage where table_schema = %s and constraint_name = %s order by ordinal_position"
    cur.execute(select_sql, (schema_name, key_name))
    result = [row[0] for row in cur.fetchall()]
    return result


def merge_dataframe_into_table(schema_name: str, table_name: str, df: pd.DataFrame, cur: psycopg.cursor.Cursor):
    """
    Load a dataframe into a table where the dataframe header is contained within the table header.
    The table must have a primary key.
    If the insert would cause a duplicate on the primary key, the existing row is updated with the new row instead.
    """
    # If the dataframe has no rows, we exit early
    if len(df) == 0:
        logger.debug("No rows to load, skipping load process")
        return
    pk_name = get_primary_key_name(schema_name=schema_name, table_name=table_name, cur=cur)
    pk_columns = get_key_columns(schema_name=schema_name, key_name=pk_name, cur=cur)
    non_pk_columns = [col for col in df.columns if col not in pk_columns]
    # We cannot merge when using the copy command
    # We will therefore create a temporary table, copy the data into the temporary table, and merge from the temporary table into the target table
    tmp_table_name = 'tmp_' + table_name
    logger.debug(f"Attempting to create temporary table: {tmp_table_name}")
    # Temporary table exist in their own schema, so we cannot use the target schema for the temporary table
    cur.execute(f"create temporary table if not exists {tmp_table_name} (like {schema_name}.{table_name}) on commit drop")
    logger.debug(f"Loading {len(df)} row(s) into {tmp_table_name}")
    load_dataframe_to_table(schema_name=None, table_name=tmp_table_name, df=df, cur=cur)
    # Once we have loaded into the temporary table using the copy command, we can generate and run the merge statement
    logger.debug(f"Merging into {table_name}")
    merge_sql = f"merge into {schema_name}.{table_name} t using {tmp_table_name} s on ({' AND '.join(['s.' + col + ' = t.' + col for col in pk_columns])})"
    merge_sql += f" when matched then update set {', '.join([col + ' = s.' + col for col in non_pk_columns])}"
    merge_sql += f" when not matched then insert ({', '.join(df.columns)}) values ({', '.join(['s.' + col for col in df.columns])})"
    logger.debug(merge_sql)
    cur.execute(merge_sql)
    # We will need to clean up by emptying the tmp table
    # Truncating is more efficient than dropping and recreating
    logger.debug(f"Truncating temporary table: {tmp_table_name}")
    cur.execute(f"truncate table {tmp_table_name}")
