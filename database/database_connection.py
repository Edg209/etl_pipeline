import os
import psycopg
import logging

logger = logging.getLogger('Database')


def get_db_connection() -> psycopg.connection.Connection:
    """Return a connection to the main database."""
    logger.debug("Connecting to main database")
    connection_string = f"host={os.environ['DB_HOST']} user={os.environ['DB_USER_USERNAME']} password={os.environ['DB_USER_PASSWORD']} dbname={os.environ['DB_NAME']}"
    conn = psycopg.connect(connection_string)
    return conn


def get_db_cursor(conn: psycopg.connection.Connection = None) -> psycopg.cursor.Cursor:
    """Return a single cursor from a database connection, using the main database if no connection is given."""
    if conn is None:
        conn = get_db_connection()
    cur = conn.cursor()
    return cur
