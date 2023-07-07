from pipeline.pipeline import PostExecutionStep, Pipeline


class CommitTransaction(PostExecutionStep):
    def execute(self, pipeline: Pipeline):
        # We only commit when all pipeline have been successfully committed.
        pipeline.conn.commit()
