from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Dict

import pandas as pd

from .account import Account
from ..common.dataframe_iterator import DataFrameIterator
from ..events import *

_log = logging.getLogger(__name__)


class Backtester(Account):

    def __init__(
            self,
            starting_balance: float = 100,
            slippage: float = 0,
            derive_quantity_slippage: float = 0.02,
            order_minimum_quantity: float = 1e-4,
            min_target_weight: float = 1e-4,
            autostart: bool = True
    ):
        super().__init__(
            starting_balance,
            slippage,
            derive_quantity_slippage,
            order_minimum_quantity,
            min_target_weight
        )

        self.open_events = 0
        if autostart:
            self.start()

    # @handler(False)
    def strategy(self, orders: pd.Series) -> Account:
        for i, e in orders.items():
            self.open_events += 1
            self.fire(e)

        return self.await_result()

    # @handler(False)
    def await_result(self):
        # TODO how can i know that we have processed all events
        self.stop()
        return self


class PandasBarBacktester(Backtester):

    def __init__(
            self,
            dataframe_provider: Callable[[Asset, datetime], pd.DataFrame],
            bar_converter: Callable[[pd.Series], Bar],
            starting_date: datetime | str,
            starting_balance: float = 100,
            slippage: float = 0,
            derive_quantity_slippage: float = 0.02,
            order_minimum_quantity: float = 1e-4,
            min_target_weight: float = 1e-4,
            autostart: bool = True
    ):
        super().__init__(
            starting_balance,
            slippage,
            derive_quantity_slippage,
            order_minimum_quantity,
            min_target_weight,
            autostart
        )

        self.dataframe_provider = dataframe_provider
        self.bar_converter = bar_converter

        self.starting_date = datetime.fromisoformat(starting_date) if isinstance(starting_date, str) else starting_date
        self.quotes: Dict[Asset, DataFrameIterator] = {}

        self.register_event(SubscribeToMarketData, TickMarketDataClock, handler=self.send_market_data)

    def send_market_data(self, event: SubscribeToMarketData | TickMarketDataClock):
        with self.lock:
            if event.asset not in self.quotes:
                df = self.dataframe_provider(event.asset, event.time)
                if len(df) <= 0:
                    _log.warning(f"empty dataframe for {event.asset}")

                self.quotes[event.asset] = DataFrameIterator(df)

        for row in self.quotes[event.asset].next_until(event.time):
            # BLOCKING: make sure we wait that each quote tick is processed
            self.fire(Quote(event.asset, row.name.to_pydatetime(), self.bar_converter(row)))
