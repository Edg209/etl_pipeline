from pipeline.pipeline import Pipeline
from intercom.conversation.pre_execution_steps import GetDatabaseConnection, GetPayloads
from intercom.conversation.pipeline_steps import RetrieveApiData, ParseApiData, LoadToDatabase
from intercom.conversation.post_execution_steps import CommitTransaction
from intercom.conversation.error_handling_steps import RollbackTransaction

IntercomConversationPipeline = Pipeline()
IntercomConversationPipeline.pre_execution_steps = [GetDatabaseConnection, GetPayloads]
IntercomConversationPipeline.pipeline_steps = [RetrieveApiData, ParseApiData, LoadToDatabase]
IntercomConversationPipeline.post_execution_steps = [CommitTransaction]
IntercomConversationPipeline.error_handling_steps = [RollbackTransaction]
