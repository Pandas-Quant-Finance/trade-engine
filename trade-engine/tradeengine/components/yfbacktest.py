from __future__ import annotations
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import pandas as pd
import yfinance
from circuits import Component, handler
from ..events import *

_log = logging.getLogger(__name__)


class YfBacktester(Component):

    def __init__(self, starting_date: datetime | str):
        super().__init__()
        self.starting_date = datetime.fromisoformat(starting_date) if isinstance(starting_date, str) else starting_date
        self.quotes: Dict[str, pd.DataFrame] = {}

    @handler(ReadyForComplexTradeEvent.__name__)
    def new_trading_day(self, assets: List[Asset], time: datetime):
        #_send_quotes(self, assets, time)
        pass

    @handler(SubscribeToQuoteProviderEvent.__name__)
    def subscribe_asset_quote_feed(self, ass: Asset, time: datetime):
        _log.debug(f"send quotes for {ass} until {time}")

        if ass.id not in self.quotes:
            self.quotes[ass.id] = yfinance.Ticker(ass.id).history(start=self.starting_date)

        data = self.quotes[ass.id]

        if time.tzinfo is None:
            time = pd.Timestamp(time)

        tosend = data[data.index <= time]
        remainder = data[data.index > time]

        for idx, row in tosend[["Open", "High", "Low", "Close", "Volume"]].iterrows():
            _ = yield self.fire(QuoteUpdatedEvent(Quote(ass, idx, Bar(*row.values))))

        self.quotes[ass.id] = remainder