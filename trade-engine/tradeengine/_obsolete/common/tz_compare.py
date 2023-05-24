from __future__ import annotations
from datetime import datetime, date
import logging
import pandas as pd

_LOG = logging.getLogger(__name__)


def timestamp_greater(
        index: pd.DatetimeIndex | str | date | datetime | pd.Timestamp,
        timestamp: str | date | datetime | pd.Timestamp
):
    return index > _eventually_localize(index, timestamp)


def timestamp_greater_equal(
        index: pd.DatetimeIndex | str | date | datetime | pd.Timestamp,
        timestamp: str | date | datetime | pd.Timestamp
):
    return index >= _eventually_localize(index, timestamp)


def timestamp_less(
        index: pd.DatetimeIndex | str | date | datetime | pd.Timestamp,
        timestamp: str | date | datetime | pd.Timestamp
):
    return index < _eventually_localize(index, timestamp)


def timestamp_less_equal(
        index: pd.DatetimeIndex | str | date | datetime | pd.Timestamp,
        timestamp: str | date | datetime | pd.Timestamp
):
    return index <= _eventually_localize(index, timestamp)


def _eventually_localize(index: pd.DatetimeIndex, timestamp: str | date | datetime | pd.Timestamp):
    index_tz = index.tzinfo if isinstance(index, datetime) else index.tz

    if index_tz is not None:
        if isinstance(timestamp, str):
            _LOG.warning(f"localize timestamp {timestamp } to {index.tz}")
            timestamp = pd.Timestamp.fromisoformat(timestamp).tz_localize(index.tz)
        elif isinstance(timestamp, (date, datetime)):
            if getattr(timestamp, 'tzinfo', None) is None:
                _LOG.warning(f"localize timestamp {timestamp} to {index_tz}")
                timestamp = pd.Timestamp(timestamp).tz_localize(index_tz)
    else:
        _LOG.warning(f"remove timezone from {timestamp}")
        timestamp = pd.Timestamp(timestamp).tz_localize(None)

    return timestamp

