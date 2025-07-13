import logging
from typing import TypedDict


class EmitterException(BaseException):
    pass


class LogRecordPair(TypedDict):
    record: logging.LogRecord
    formatted_msg: str
