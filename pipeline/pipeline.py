"""
This file contains each of the classes used in ETL pipelines. It contains generic logic not specific to any particular ETL pipeline.
"""

from __future__ import annotations
import logging
from abc import ABCMeta, abstractmethod
from pipeline.exceptions import StopPipelineException, StopPayloadException

logger = logging.getLogger('Pipeline')


class Payload(object):
    """
    A payload represents a set of data that the pipeline will process by running each step in order.
    As it is processed its attributes will be populated with data used in the payload.
    """


class PipelineStep(metaclass=ABCMeta):
    """
    A PipelineStep represents a single step of logic that is applied to each payload.
    It must have a method named execute that takes as input a single Payload object.
    The execute method must modify the attributes of the Payload object instead of returning an output.
    """

    @abstractmethod
    def execute(self, payload: Payload):
        pass


class PreExecutionStep(metaclass=ABCMeta):
    """
    A PreExecutionStep represents a single step of logic that is applied before all payloads are processed.
    It is executed exactly once, no matter how many payloads are processed.
    It must have a method named execute that takes as input a single Pipeline object and returns no output.
    """

    @abstractmethod
    def execute(self, pipeline: Pipeline):
        pass


class PostExecutionStep(metaclass=ABCMeta):
    """
    A PostExecutionStep represents a single step of logic that is applied after all payloads are processed successfully.
    It is executed exactly once, no matter how many payloads are processed.
    It must have a method named execute that takes as input a single Pipeline object and returns no output.
    """

    @abstractmethod
    def execute(self, pipeline: Pipeline):
        pass


class ErrorHandlingStep(metaclass=ABCMeta):
    """
    An ErrorHandlingStep represents a single step of logic that is applied if a pipeline encounters an error.
    It is executed exactly once, no matter how many payloads are processed.
    It must have a method named execute that takes as input a single Pipeline object and returns no output.
    """

    @abstractmethod
    def execute(self, pipeline: Pipeline):
        pass


class Pipeline(object):
    """
    A Pipeline represents an ETL pipeline that will process payloads by applying logical steps to them.
    Each logic step is an in instance of a PipelineStep object, and each payload is an instance of a Payload object.
    The Payloads are processed in the order provided, with each payload fully processed before moving onto the next one.
    Once each payload has been processed, the cleanup steps are run in the order provided.
    """

    payloads: list[Payload]
    pre_execution_steps: list[type[PreExecutionStep]]
    pipeline_steps: list[type[PipelineStep]]
    post_execution_steps: list[type[PostExecutionStep]]
    error_handling_steps: list[type[ErrorHandlingStep]]

    def __repr__(self):
        return str(self.__dict__)

    def execute(self):
        """For each Payload, process that Payload by running each PipelineStep for that Payload."""
        # If there are any errors while executing the payloads or during the post execution steps, we log the error, run the error handler steps and reraise the error.
        # If we encounter a StopPipelineException in the pre execution steps or when running payloads, we jump straight to the post execution steps and do not consider it an error.
        try:
            try:
                self.run_pre_execution_steps()
                self.run_all_payloads()
            except StopPipelineException as e:
                logger.info(f"Stopping pipeline early: {e.msg}")
            self.run_post_execution_steps()
        except Exception as e:
            logger.error(f"Encountered unexpected error: {e}")
            self.run_error_handling()
            raise e

    def run_pre_execution_steps(self):
        # Each of the pre execution steps is run in order
        logger.info(f"{len(self.pre_execution_steps)} pre execution step(s) to execute.")
        logger.debug(f"Pipeline state: {self.__dict__}")
        for pre_execution_step in self.pre_execution_steps:
            logger.info(f"Starting pre execution step: {pre_execution_step.__name__}")
            pre_execution_step().execute(self)
            logger.debug(f"Pipeline state: {repr(self)}")
        logger.info("Processed all pre execution steps successfully.")

    def run_all_payloads(self):
        # The normal pythonic way of doing this would be to iterate over payloads and call run_payload for each element in the list.
        # If this is done, early payloads will consume memory that will not be freed until all payloads have been processed.
        # If a large number of payloads are processed (for instance, if a backlog of historical data is processed), this can cause memory issues.
        # Instead, the below method is used, which will guarantee that no more than one fully processed payload will be stored in memory at once.
        if len(self.payloads) == 0:
            logger.info("No payloads to process, exiting without doing anything.")
            return
        while len(self.payloads) > 0:
            logger.info(f"{len(self.payloads)} payload(s) to process.")
            self.run_payload(self.payloads[0])
            del self.payloads[0]
        logger.info("Processed all payloads successfully.")

    def run_payload(self, payload: Payload):
        # Each of the pipeline steps is run in order
        logger.debug(f"Payload state: {self.__dict__}")
        for pipeline_step in self.pipeline_steps:
            logger.info(f"Starting pipeline step: {pipeline_step.__name__}")
            try:
                pipeline_step().execute(payload)
                logger.info(f"Finished pipeline step: {pipeline_step.__name__}")
            except StopPayloadException as e:
                logger.info(f"Stopping payload early: {e.msg}")
                break
            logger.debug(f"Payload state: {payload.__dict__}")
        logger.info(f"Processed payload successfully.")

    def run_post_execution_steps(self):
        # Each of the post execution steps is run in order
        logger.info(f"{len(self.post_execution_steps)} post execution step(s) to execute.")
        logger.debug(f"Pipeline state: {self.__dict__}")
        for post_execution_step in self.post_execution_steps:
            logger.info(f"Starting post execution step: {post_execution_step.__name__}")
            post_execution_step().execute(self)
            logger.debug(f"Pipeline state: {repr(self)}")
        logger.info("Processed all post execution steps successfully.")

    def run_error_handling(self):
        # Each of the error handling steps is run in order
        logger.info(f"{len(self.error_handling_steps)} error handling step(s) to execute.")
        logger.debug(f"Pipeline state: {self.__dict__}")
        for error_handling_step in self.error_handling_steps:
            logger.info(f"Starting error handling step: {error_handling_step.__name__}")
            error_handling_step().execute(self)
            logger.debug(f"Pipeline state: {self.__dict__}")
        logger.info("Processed all error handling steps successfully.")
