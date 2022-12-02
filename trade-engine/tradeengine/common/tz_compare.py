from __future__ import annotations
from datetime import datetime, date
import logging
import pandas as pd

_LOG = logging.getLogger(__name__)


def timestamp_greater(index: pd.DatetimeIndex, timestamp: str | date | datetime | pd.Timestamp):
    return index > _eveutually_localize(index, timestamp)


def timestamp_greater_equal(index: pd.DatetimeIndex, timestamp: str | date | datetime | pd.Timestamp):
    return index >= _eveutually_localize(index, timestamp)


def timestamp_less(index: pd.DatetimeIndex, timestamp: str | date | datetime | pd.Timestamp):
    return index < _eveutually_localize(index, timestamp)


def timestamp_less_equal(index: pd.DatetimeIndex, timestamp: str | date | datetime | pd.Timestamp):
    return index <= _eveutually_localize(index, timestamp)


def _eveutually_localize(index: pd.DatetimeIndex, timestamp: str | date | datetime | pd.Timestamp):
    if index.tz is not None:
        if isinstance(timestamp, str):
            _LOG.warning(f"localize timestamp {timestamp } to {index.tz}")
            timestamp = pd.Timestamp.fromisoformat(timestamp).tz_localize(index.tz)
        elif isinstance(timestamp, (date, datetime)):
            if getattr(timestamp, 'tzinfo', None) is None:
                _LOG.warning(f"localize timestamp {timestamp} to {index.tz}")
                timestamp = pd.Timestamp(timestamp).tz_localize(index.tz)
    else:
        _LOG.warning(f"remove timezone from {timestamp}")
        timestamp = pd.Timestamp(timestamp).tz_localize(None)

    return timestamp

