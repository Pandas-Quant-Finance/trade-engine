from __future__ import annotations

import logging
from datetime import datetime

import yfinance

from ..events import Bar

_log = logging.getLogger(__name__)

from .backtester import PandasBarBacktester


class YfBacktester(PandasBarBacktester):

    def __init__(self, starting_date: datetime | str, starting_balance: float = 100):
        super().__init__(
            lambda asset, time: yfinance.Ticker(asset.id).history(start=time),
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            starting_date,
            starting_balance
        )
