import logging
import sys
import threading
from logging import LogRecord
from time import time

import yandexcloud
from yandex.cloud.logging.v1.log_entry_pb2 import Destination
from yandex.cloud.logging.v1.log_ingestion_service_pb2_grpc import LogIngestionServiceStub
from yandex.cloud.logging.v1.log_resource_pb2 import LogEntryResource

from .emitters import Emitter, LogRecordPair


class YandexCloudHandler(logging.Handler):
    """
    Handler used for sending logs to Yandex Cloud Logging service
    """

    def __init__(
            self,
            credentials: dict[str, str],
            log_group_id: str = None,
            folder_id: str = None,
            *,
            resource_type: str = None,
            resource_id: str = None,
            log_batch_size: int = 10,
            commit_period: int = 10,
            **kwargs,
    ):
        """
        :param credentials: dictionary of credentials (oauth_token, iam_token and etc.) for setting up connection to Yandex Cloud
        :param log_group_id: group's id where logs should be sent
        :param folder_id: folder's id where logs should be sent (you can provide only one of the group or folder ids)
        :param resource_type: resource name for log record
        :param resource_id: resource id for log record
        :param log_batch_size: number of logs in each batch that is going to be sent to Yandex via gRPC client
        :param commit_period: time period in seconds between write requests to Yandex if batch hasn't been filled fully
        :param kwargs:
        """
        super().__init__(**kwargs)

        self.log_batch_size = log_batch_size
        self.commit_period = commit_period
        self.logs_buffer = []
        self.last_commit = time()

        client = yandexcloud.SDK(**credentials).client(LogIngestionServiceStub)
        destination = Destination(log_group_id=log_group_id, folder_id=folder_id)
        resource = LogEntryResource(type=resource_type, id=resource_id)

        self.emitter = Emitter(
            client=client,
            destination=destination,
            resource=resource,
        )

    def emit(self, record: LogRecord):
        try:
            new_entry = LogRecordPair(record=record, formatted_msg=self.format(record))
            self.logs_buffer.append(new_entry)

            if len(self.logs_buffer) >= self.log_batch_size or self.period_passed:
                logs = self.logs_buffer[:]  # make slice copy so it doesn't change in thread
                th = threading.Thread(target=self.emitter.send, kwargs={"log_records": logs})
                th.start()

                self.empty_buffer()
                self.set_commit_time()

        except Exception:
            record.exc_info = sys.exc_info()
            self.handleError(record)

    def handleError(self, record):
        self.write_to_console()

        super().handleError(record)

    def write_to_console(self):
        logs = [str(log["formatted_msg"]) + "\n" for log in self.logs_buffer]
        sys.stdout.writelines(logs)

    def empty_buffer(self):
        self.logs_buffer = []

    def set_commit_time(self):
        self.last_commit = time()

    @property
    def period_passed(self) -> bool:
        return time() >= self.last_commit + self.commit_period

    def __del__(self):
        self.write_to_console()





