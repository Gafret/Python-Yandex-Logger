import logging
import sys
from typing import Iterable, TypedDict

from yandex.cloud.logging.v1.log_entry_pb2 import IncomingLogEntry, Destination, LogLevel
from yandex.cloud.logging.v1.log_ingestion_service_pb2 import WriteRequest
from yandex.cloud.logging.v1.log_resource_pb2 import LogEntryResource
from yandexcloud._sdk import Client

from src.logging_yandex.utils import get_curr_timestamp

TRACE = 60
DEFAULT_LOG_LEVEL: LogLevel = LogLevel.WARN  # used when there is no log level with 'levelno' from LogRecord in _MAPPED_LOG_LEVELS

_MAPPED_LOG_LEVELS = {
    logging.FATAL: LogLevel.FATAL,
    logging.ERROR: LogLevel.ERROR,
    logging.WARN: LogLevel.WARN,
    logging.INFO: LogLevel.INFO,
    logging.DEBUG: LogLevel.DEBUG,
    logging.NOTSET: LogLevel.LEVEL_UNSPECIFIED,
    TRACE: LogLevel.TRACE,
}

logging.addLevelName(TRACE, "TRACE")


class EmitterException(BaseException):
    pass


class LogRecordPair(TypedDict):
    record: logging.LogRecord
    formatted_msg: str


class Emitter:
    """
    Emitter class that sends write requests to Yandex Cloud Logging service
    """

    def __init__(self, client: Client, destination: Destination, resource: LogEntryResource):
        self.client = client
        self.destination = destination
        self.resource = resource

    def send(self, log_records: Iterable[LogRecordPair]):
        log_entries = [self.build_payload(record) for record in log_records]
        request = self.build_request(log_entries)

        try:
            self.client.Write(request)
        except Exception as err:
            self.write_to_console(log_records)
            raise EmitterException("Error occurred while trying "
                                   "to send a request to Yandex (logs were flushed to stdout)", err)

    def write_to_console(self, log_records: Iterable[LogRecordPair]):
        logs = [str(log["formatted_msg"]) + "\n" for log in log_records]
        sys.stdout.writelines(logs)

    def build_payload(self, log_pair: LogRecordPair) -> IncomingLogEntry:
        record = log_pair["record"]
        formatted_msg = log_pair["formatted_msg"]

        timestamp = get_curr_timestamp()
        level = _MAPPED_LOG_LEVELS.get(record.levelno, DEFAULT_LOG_LEVEL)
        extra = record.__dict__.get("json_payload", None)

        log_entry = IncomingLogEntry(
            timestamp=timestamp,
            level=level,
            message=formatted_msg,
            json_payload=extra,
        )

        return log_entry

    def build_request(self, log_entries: Iterable[IncomingLogEntry]) -> WriteRequest:
        write_request = WriteRequest(
            destination=self.destination,
            resource=self.resource,
            entries=log_entries,
        )

        return write_request
