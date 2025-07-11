from google.protobuf import timestamp_pb2
from typing import Type


def get_curr_timestamp() -> Type[timestamp_pb2.Timestamp]:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.GetCurrentTime()

    return timestamp
