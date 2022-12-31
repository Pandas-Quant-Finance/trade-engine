from __future__ import annotations
import logging
from abc import abstractmethod
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from itertools import chain
from threading import Lock
from typing import Dict, List, Any, Tuple

import pandas as pd
from .orderbook import OrderBook
from .portfolio import Portfolio
from ..common.tz_compare import timestamp_greater_equal
from ..events import *
from .component import Component
from ..events.data import TickMarketDataClock

_log = logging.getLogger(__name__)


class Account(Component):

    def __init__(self, starting_balance: float = 100, slippage: float = 0, derive_quantity_slippage: float = 0.02):
        super().__init__()
        self._starting_balance = starting_balance
        self.derive_quantity_slippage = derive_quantity_slippage

        self.lock = Lock()
        self.portfolio = Portfolio()
        self.orderbook = OrderBook(slippage)

        self.cash_balance = self._starting_balance
        self.cash_timeseries: Dict[datetime, float] = {datetime.fromisoformat('0001-01-01'): self._starting_balance}
        self.latest_quotes: Dict[Asset, Quote] = {}

        self.register(TradeExecution, handler=self.on_trade_execution)
        self.register(TargetWeights, handler=self.place_target_weights_oder)
        self.register(MaximumOrder, handler=self.place_maximum_order)
        self.register(CloseOrder, handler=self.close_position)
        self.register(Quote, handler=self.on_quote_update)

    def on_quote_update(self, quote: Quote):
        with self.lock:
            if quote.asset in self.latest_quotes:
                if timestamp_greater_equal(quote.time, self.latest_quotes[quote.asset].time):
                    self.latest_quotes[quote.asset] = quote
            else:
                self.latest_quotes[quote.asset] = quote

    def on_trade_execution(self, trade: TradeExecution):
        with self.lock:
            self.cash_balance += (-trade.quantity * trade.price)
            self.cash_timeseries[trade.time] = self.cash_balance

    def place_order(self, order: Order):
        self.orderbook.place_order(order)

    def place_maximum_order(self, order: MaximumOrder):
        # BLOCKING: make sure we have the latest market data
        self.fire(TickMarketDataClock(order.asset, order.valid_from))

        # get maximum possible capital and place order
        balance = self.cash_balance * (1 - self.derive_quantity_slippage)

        # use derive_quantity_slippage to allow market movements from this tick to the next one
        with self.lock:
            price = self.latest_quotes[order.asset].get_price(order.quantity, 'last')

        self.fire(Order(order.asset, balance / price, order.limit, order.valid_from, order.valid_to, order.position_id))

    def place_target_weights_oder(self, target_weights: TargetWeights, slippage: float = 0.02, min_weight=1e-3):
        # BLOCKING: make sure we have the latest market data
        for asset in target_weights.asset_weights.keys():
            self.fire(TickMarketDataClock(asset, target_weights.valid_from))

        # TODO calculate the quantity from the weights
        #  find the difference between the current position quantity and the target quantity
        #  place orders of quantity if the quantity is not negligible small
        #  use derive_quantity_slippage
        #  use with self.lock: for self.quotes access
        pass

    def close_position(self, order: CloseOrder):
        # BLOCKING: we need all the latest quotes such that we can calculate the current weights
        for asset in chain(self.portfolio.assets, self.orderbook.assets):
            self.fire(TickMarketDataClock(asset, order.valid_from))

        positions = self.portfolio.get_positions()
        if order.position is None:
            _log.warning("we close absolutely everything!")
            for ass, poss in positions.items():
                for pid, pos in poss.items():
                    self.fire(Order(ass, -pos.quantity, order.limit, order.valid_from, position_id=pid))
        elif isinstance(order.position, Asset):
            for pid, pos in positions[order.position].items():
                self.fire(Order(pos.asset, pos.quantity, order.limit, order.valid_from, position_id=pid))
        else:
            for ass, poss in positions.items():
                if order.position in poss:
                    pos = poss[order.position]
                    self.fire(Order(ass, pos.quantity, order.limit, order.valid_from, position_id=order.position))

    def get_current_weights(self, time: datetime | str = datetime.now(), pid: str = '') -> Dict[Asset, float]:
        # BLOCKING: we need all the latest quotes such that we can calculate the current weights
        for asset in chain(self.portfolio.assets, self.orderbook.assets):
            self.fire(TickMarketDataClock(asset, time))

        with self.lock:
            return self.portfolio.get_weights(self.cash_balance)

    def get_history(self, time: datetime | str = datetime.now()):
        # BLOCKING: we need all the latest quotes such that we can calculate the current weights
        for asset in chain(self.portfolio.assets, self.orderbook.assets):
            self.fire(TickMarketDataClock(asset, time))

        position_ts = self.portfolio.get_timeseries().swaplevel(0, 1, axis=1)
        if len(position_ts) <= 0:
            return pd.DataFrame({})

        # join pnl %
        pnl_pct = position_ts[["pnl", "realized_pnl", "unrealized_pnl"]] * (1 / self._starting_balance)
        position_ts = position_ts.join(pnl_pct.rename(columns=lambda x: f"{x}_%", level=0))

        # join cash and cash %
        cash = pd.Series(self.cash_timeseries, name=('balance', '$CASH$'))
        position_ts = position_ts.join(
            pd.concat([cash, (cash / self._starting_balance).rename(('%', '$CASH$'))], axis=1),
            how = 'outer'
        )

        # return timeseries
        return position_ts.swaplevel(0, 1, axis=1).sort_index(axis=1, level=0)


    @property
    def total_balance(self, time = None):
        if time is not None:
            # BLOCKING: make sure we have the latest quotes
            for asset in chain(self.orderbook.assets, self.portfolio.assets):
                self.fire(TickMarketDataClock(asset, time))

        with self.lock:
            return self.cash_balance + self.portfolio.total_position_value

