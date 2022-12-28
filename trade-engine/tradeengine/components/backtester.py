from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Dict

import pandas as pd

from .account import Account
from ..common.dataframe_iterator import DataFrameIterator
from ..events import *

_log = logging.getLogger(__name__)


class PandasBarBacktester(Account):

    def __init__(
            self,
            dataframe_provider: Callable[[Asset, datetime], pd.DataFrame],
            bar_converter: Callable[[pd.Series], Bar],
            starting_date: datetime | str,
            starting_balance: float = 100,
            slippage: float = 0
    ):
        super().__init__(starting_balance, slippage)
        self.dataframe_provider = dataframe_provider
        self.bar_converter = bar_converter

        self.starting_date = datetime.fromisoformat(starting_date) if isinstance(starting_date, str) else starting_date
        self.quotes: Dict[Asset, DataFrameIterator] = {}
        self.register(SubscribeToMarketData, TickMarketDataClock, handler=self.send_market_data)

    def send_market_data(self, event: SubscribeToMarketData | TickMarketDataClock):
        if event.asset not in self.quotes:
            self.quotes[event.asset] = DataFrameIterator(self.dataframe_provider(event.asset, event.time))

        for row in self.quotes[event.asset].next_until(event.time):
            self.fire(Quote(event.asset, row.name.to_datetime(), self.bar_converter(row)))
