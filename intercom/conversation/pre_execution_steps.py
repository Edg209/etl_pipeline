from pipeline.pipeline import Pipeline, PreExecutionStep, Payload
from database.database_connection import get_db_connection
from intercom.api import get_conversation_max_updated_timestamp, get_conversation_ids_last_updated_between_timestamps
from pipeline.exceptions import StopPipelineException


class GetDatabaseConnection(PreExecutionStep):
    def execute(self, pipeline: Pipeline):
        # We will want to make sure that the all the database updates are contained in a single transaction.
        # Therefore, we will use a single connection and cursor for each of the payloads.
        # This will be committed in a post execution step.
        pipeline.conn = get_db_connection()
        pipeline.cur = pipeline.conn.cursor()


class GetPayloads(PreExecutionStep):
    def execute(self, pipeline: Pipeline):
        # A payload will be a single conversation that has been updated since the last time the pipeline was run
        # A conversation is considered updated if it has an updated_at which is strictly greater than the largest value already in the database
        # Because we will need to query the API multiple times, it is possible that updates happen between API queries
        # We therefore need to determine an end timestamp that is not in the future and ignore any updates after that end timestamp
        # This allows us to prioritize not missing any data at the cost of updates potentially being captured by a later pipeline run than the first one after the update is performed
        db_schema = 'intercom'
        pipeline.cur.execute(f"select coalesce(max(updated_at), 0) from {db_schema}.conversations")
        start_timestamp = pipeline.cur.fetchone()[0]
        end_timestamp = get_conversation_max_updated_timestamp(start_timestamp=start_timestamp)
        # If we have not found any updates, we end the pipeline without continuing
        if end_timestamp is None:
            raise StopPipelineException('No updates to conversations found, stopping pipeline.')
        updated_conversation_ids = get_conversation_ids_last_updated_between_timestamps(start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        # We construct a payload for each of the conversations to update
        payloads = []
        for conversation_id in updated_conversation_ids:
            payload = Payload()
            # We will need to conversation id to retrieve the conversation data, and the start and end timestamps for identifying new conversation components
            payload.conversation_id = conversation_id
            payload.start_timestamp = start_timestamp
            payload.end_timestamp = end_timestamp
            # We will need to make the cursor from the pipeline available to the payload
            payload.cur = pipeline.cur
            payload.db_schema = db_schema
            payloads.append(payload)
        pipeline.payloads = payloads
