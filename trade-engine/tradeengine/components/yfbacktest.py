from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable

import pandas as pd
import yfinance

from . import Account
from ..events import *

_log = logging.getLogger(__name__)


class PandasBarBacktester(Account):

    def __init__(
            self,
            dataframe_provider: Callable[[Asset, datetime], pd.DataFrame],
            starting_date: datetime | str,
            starting_balance: float = 100
    ):
        super().__init__(starting_balance)
        self.dataframe_provider = dataframe_provider
        self.starting_date = datetime.fromisoformat(starting_date) if isinstance(starting_date, str) else starting_date
        self.quotes: Dict[str, pd.DataFrame] = {}
        self.register(SubscribeToMarketData, handler=self.prepare_to_trade)

    def prepare_to_trade(self, pre_order: SubscribeToMarketData):
        # implement me
        pass

    def prepare_to_trade_(self, assets: List[Asset], time: datetime):
        if self.quote_date is not None:
            if time.tzinfo is None:
                time = pd.Timestamp(time, tz=self.quote_date.tzinfo)

            assert time >= self.quote_date, \
            f"cant trade in the past! {time} vs {self.quote_date}"

        for ass in assets:
            _log.debug(f"send quotes for {ass} until {time}")

            # fetch data
            if ass.id not in self.quotes:
                self.quotes[ass.id] = self.dataframe_provider(ass, self.starting_date)
            data = self.quotes[ass.id]
            if len(data) <= 0: continue

            # fix timezones
            if time.tzinfo is None:
                time = pd.Timestamp(time, tz=data.index[0].tz)

            # check already sent everything we have
            if data.index[0] > time:
                continue

            # split dataset for data to send and data to keep for later
            tosend = data[data.index <= time]
            remainder = data[data.index > time]

            # send quote update, keep remaining data
            for idx, row in tosend[["Open", "High", "Low", "Close", "Volume"]].iterrows():
                self.quote_update(Quote(ass, idx, Bar(*row.values)))
            self.quotes[ass.id] = remainder


class YfBacktester(PandasBarBacktester):

    def __init__(self, starting_date: datetime | str, starting_balance: float = 100):
        super().__init__(
            lambda asset, time: yfinance.Ticker(asset.id).history(start=time),
            starting_date,
            starting_balance
        )
