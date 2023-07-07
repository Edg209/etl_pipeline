from pipeline.pipeline import ErrorHandlingStep, Pipeline


class RollbackTransaction(ErrorHandlingStep):
    def execute(self, pipeline: Pipeline):
        # If any of the payloads encountered an error, we want to rollback the transaction, so we do not have a partially updated database table.
        pipeline.conn.rollback()
