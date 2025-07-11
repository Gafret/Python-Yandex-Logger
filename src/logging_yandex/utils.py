import sys

from google.protobuf import timestamp_pb2
from typing import Type, Iterable

from .types import LogRecordPair


def get_curr_timestamp() -> Type[timestamp_pb2.Timestamp]:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.GetCurrentTime()

    return timestamp


def write_to_console(log_records: Iterable[LogRecordPair]):
    logs = [str(log["formatted_msg"]) + "\n" for log in log_records]
    sys.stdout.writelines(logs)
