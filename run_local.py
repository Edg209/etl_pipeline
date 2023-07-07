"""
This file is used for running pipelines locally.

For the purpose of this exercise, the only pipeline is the Intercom Conversation pipeline. Future developments may add more pipelines.
"""

import os
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def load_env_variables():
    # We store the credentials that would normally be in environment variables in a json to set up when running locally.
    with open("credentials.json") as credentials_file:
        credentials_json = json.load(credentials_file)
        for cred in credentials_json:
            logger.info(f"Setting environment variable {cred}")
            os.environ[cred] = credentials_json[cred]


def recreate_db_objects():
    # We drop and recreate the tables for testing purposes
    table_folder = r"database_objects\intercom\tables"
    index_folder = r"database_objects\intercom\indexes"
    from database.database_connection import get_db_connection
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('set search_path = "intercom"')
    cur.execute("select tablename from pg_tables where schemaname = 'intercom'")
    for rec in cur.fetchall():
        logger.info(f"dropping table {rec[0]}")
        cur.execute(f"drop table {rec[0]}")
    for file_name in os.listdir(table_folder):
        logger.info(f"Creating table {file_name}")
        with open(os.path.join(table_folder, file_name)) as sql_file:
            cur.execute(sql_file.read())
    for file_name in os.listdir(index_folder):
        logger.info(f"Creating index {file_name}")
        with open(os.path.join(index_folder, file_name)) as sql_file:
            cur.execute(sql_file.read())
    conn.commit()


if __name__ == "__main__":
    load_env_variables()
    recreate_db_objects()
    from intercom.conversation.pipeline import IntercomConversationPipeline
    IntercomConversationPipeline.execute()
