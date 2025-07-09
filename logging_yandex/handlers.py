import logging
import sys
from time import time
from logging import LogRecord
from typing import Type

import yandexcloud
from google.protobuf import timestamp_pb2
from yandex.cloud.logging.v1.log_entry_pb2 import IncomingLogEntry, Destination, LogLevel
from yandex.cloud.logging.v1.log_ingestion_service_pb2 import WriteRequest
from yandex.cloud.logging.v1.log_ingestion_service_pb2_grpc import LogIngestionServiceStub
from yandex.cloud.logging.v1.log_resource_pb2 import LogEntryResource
from yandexcloud._sdk import Client


TRACE = 60

_MAPPED_LOG_LEVELS = {
    logging.FATAL: LogLevel.FATAL,
    logging.ERROR: LogLevel.ERROR,
    logging.WARN: LogLevel.WARN,
    logging.INFO: LogLevel.INFO,
    logging.DEBUG: LogLevel.DEBUG,
    logging.NOTSET: LogLevel.LEVEL_UNSPECIFIED,
    TRACE: LogLevel.TRACE,
}


def get_curr_timestamp() -> Type[timestamp_pb2.Timestamp]:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.GetCurrentTime()

    return timestamp


class YandexCloudHandler(logging.Handler):

    client: Client | None = None

    def __new__(cls, credentials: dict[str, str], **kwargs):
        if cls.client is None:
            cls.client = yandexcloud.SDK(**credentials).client(LogIngestionServiceStub)

        return super().__new__(cls)

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
        super().__init__(**kwargs)

        self.destination = Destination(log_group_id=log_group_id, folder_id=folder_id)
        self.resource = LogEntryResource(type=resource_type, id=resource_id)

        self.log_batch_size = log_batch_size
        self.commit_period = commit_period
        self.logs_buffer = []
        self.last_commit = time()

    def emit(self, record: LogRecord):
        try:
            new_entry = self.build_payload(record)
            self.logs_buffer.append(new_entry)

            if len(self.logs_buffer) >= self.log_batch_size or time() >= self.last_commit + self.commit_period:
                request = self.build_request()
                self.client.Write(request)
                self.empty_buffer()
                self.last_commit = time()
        except Exception:
            record.exc_info = sys.exc_info()
            self.handleError(record)

    def build_payload(self, record: LogRecord) -> IncomingLogEntry:
        formatted_msg = self.format(record)
        timestamp = get_curr_timestamp()
        level = _MAPPED_LOG_LEVELS[record.levelno]
        extra = record.__dict__.get("json_payload", None)

        log_entry = IncomingLogEntry(timestamp=timestamp, level=level, message=formatted_msg, json_payload=extra)

        return log_entry

    def handleError(self, record):
        logs = [str(log) + "\n" for log in self.logs_buffer]
        sys.stdout.writelines(logs)

        super().handleError(record)

    def concurrent_write(self, request):
        print("WRITE")
        self.client.Write(request)
        self.empty_buffer()
        self.last_commit = time()

    def build_request(self) -> WriteRequest:
        write_request = WriteRequest(
            destination=self.destination,
            resource=self.resource,
            entries=self.logs_buffer,
        )

        return write_request

    def empty_buffer(self):
        self.logs_buffer = []



