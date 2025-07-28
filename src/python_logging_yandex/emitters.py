import logging
from typing import Iterable, Any
from abc import ABC, abstractmethod

from yandex.cloud.logging.v1.log_entry_pb2 import IncomingLogEntry, Destination, LogLevel
from yandex.cloud.logging.v1.log_ingestion_service_pb2 import WriteRequest
from yandex.cloud.logging.v1.log_resource_pb2 import LogEntryResource

from .types import LogRecordPair, EmitterException
from .utils import get_curr_timestamp, write_to_console

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


class BaseEmitter(ABC):

    @abstractmethod
    def send(self, log_records: Iterable[LogRecordPair]):
        pass


class Emitter(BaseEmitter):
    """
    Emitter class that sends write requests to Yandex Cloud Logging service
    """

    def __init__(self, client: Any, destination: Destination, resource: LogEntryResource):
        self.client = client
        self.destination = destination
        self.resource = resource

    def send(self, log_records: Iterable[LogRecordPair]):
        log_entries = [self.build_payload(record) for record in log_records]
        request = self.build_request(log_entries)

        try:
            self.client.Write(request)
        except Exception as err:
            write_to_console(log_records)
            raise EmitterException("Error occurred while trying "
                                   "to send a request to Yandex (logs were flushed to stdout)", err)

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


class LocalEmitter(BaseEmitter):

    def send(self, log_records: Iterable[LogRecordPair]):
        for log in log_records:
            print(f"USING LOCAL ENVIRONMENT: {log['formatted_msg']}")
