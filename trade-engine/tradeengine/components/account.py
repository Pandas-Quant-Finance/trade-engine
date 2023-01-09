from __future__ import annotations

import logging
from datetime import datetime, timedelta
from itertools import chain
from threading import Lock
from typing import Dict

import pandas as pd

from .component import Component
from .orderbook import OrderBook
from .portfolio import Portfolio
from ..common.tz_compare import timestamp_greater_equal
from ..events import *
from ..events.data import TickMarketDataClock

_log = logging.getLogger(__name__)


class Account(Component):

    def __init__(
            self,
            starting_balance: float = 100,
            slippage: float = 0,
            derive_quantity_slippage: float = 0.02,
            order_minimum_quantity: float = 1e-4,
            min_target_weight: float = 1e-4,
    ):
        super().__init__()
        self._starting_balance = starting_balance
        self.derive_quantity_slippage = derive_quantity_slippage
        self.min_target_weight = min_target_weight

        self.lock = Lock()
        self.portfolio = Portfolio().register(self)
        self.orderbook = OrderBook(slippage, order_minimum_quantity).register(self)

        self.cash_balance = self._starting_balance
        self.cash_timeseries: Dict[datetime, float] = {None: self._starting_balance}
        self.latest_quotes: Dict[Asset, Quote] = {}

        self.register_event(TradeExecution, handler=self.on_trade_execution)
        self.register_event(TargetWeights, handler=self.place_target_weights_oder)
        self.register_event(MaximumOrder, handler=self.place_maximum_order)
        self.register_event(CloseOrder, handler=self.on_close_position)
        self.register_event(Quote, handler=self.on_quote_update)

    # @handler(False)
    def place_all_orders(self, s: pd.Series):
        start = datetime.now()
        for date, item in s.items():
            if isinstance(item, Order):
                self.place_order(item)
            elif isinstance(item, MaximumOrder):
                self.place_maximum_order(item)
            elif isinstance(item, TargetWeights):
                self.place_target_weights_oder(item)
            elif isinstance(item, CloseOrder):
                self.place_close_position_order(item)
            else:
                raise ValueError(f"Unknown order of type {type(item)}")

        _log.warning(f"placed {len(s)} orders in {(datetime.now() - start).seconds} seconds")

    # @handler(False)
    def place_order(self, order: Order):
        self.fire(order)

    # @handler(False)
    def place_maximum_order(self, order: MaximumOrder):
        # BLOCKING: make sure we have the latest market data
        self.fire(TickMarketDataClock(order.asset, order.valid_from))

        with self.lock:
            # get maximum possible capital and place order
            # use derive_quantity_slippage to allow market movements from this tick to the next one
            balance = self.cash_balance * (1 - self.derive_quantity_slippage)
            price = self.latest_quotes[order.asset].get_price(order.quantity, 'last')

        self.fire(Order(order.asset, balance / price, order.limit, order.valid_from, order.valid_to, order.position_id))

    # @handler(False)
    def place_target_weights_oder(self, target_weights: TargetWeights):
        # BLOCKING: make sure we have the latest market data
        for asset in target_weights.asset_weights.keys():
            self.fire(TickMarketDataClock(asset, target_weights.valid_from))
            if not asset in self.latest_quotes:
                _log.warning(f"seems we have no price for {asset} <= {target_weights.valid_from}")

        orders = []
        with self.lock:
            total_balance = (self.cash_balance + self.portfolio.total_position_value) * (1 - self.derive_quantity_slippage)
            # print(total_balance)

            for asset, weight in target_weights.asset_weights.items():
                if abs(weight) < self.min_target_weight:
                    _log.warning(f"skip trade for {asset} because weight |{weight}| < {self.min_target_weight}")
                    continue

                pid = target_weights.position_id + "/" + asset.id
                target_quantity = (total_balance * weight) / self.latest_quotes[asset].get_price(weight, 'last')
                current_quantity = self.portfolio.get_quantity(asset, pid)
                trade_quantity = target_quantity - current_quantity

                orders.append(
                    Order(
                        asset,
                        trade_quantity,
                        valid_from=target_weights.valid_from,
                        valid_to=target_weights.valid_to,
                        position_id=pid
                    )
                )

        for o in orders:
            self.fire(o)

    # @handler(False)
    def place_close_position_order(self, order: CloseOrder):
        self.fire(order)

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

    def on_close_position(self, order: CloseOrder):
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

    # @handler(False)
    def get_current_weights(self, time: datetime | str = datetime.now(), pid: str = '') -> Dict[Asset, float]:
        # BLOCKING: we need all the latest quotes such that we can calculate the current weights
        for asset in chain(self.portfolio.assets, self.orderbook.assets):
            self.fire(TickMarketDataClock(asset, time))

        with self.lock:
            return self.portfolio.get_weights(self.cash_balance)

    # @handler(False)
    def get_history(self, time: datetime | str = datetime.now()):
        # BLOCKING: we need all the latest quotes such that we can calculate the current weights
        for asset in chain(self.portfolio.assets, self.orderbook.assets):
            self.fire(TickMarketDataClock(asset, time))

        position_ts = self.portfolio.get_timeseries()
        position_ts = position_ts.swaplevel(0, 1, axis=1)
        if len(position_ts) <= 0:
            return pd.DataFrame({})

        with self.lock:
            cash = pd.Series(self.cash_timeseries, name=('balance', '$CASH$'))
            cash.index = cash.index.fillna(position_ts.index[0] - timedelta(days=1))

        # join cash and cash %
        position_ts = pd.concat(
            [
                pd.concat([position_ts[[]], cash, (cash / self._starting_balance).rename(('%', '$CASH$'))], axis=1).ffill(),
                position_ts
            ],
            join='outer',
            axis=1,
        )

        # join pnl %
        pnl_pct = (position_ts[["value"]] + position_ts[[('balance', '$CASH$')]].values) * (1 / self._starting_balance) - 1
        position_ts = position_ts.join(pnl_pct.rename(columns=lambda x: f"pnl_%", level=0))

        # return timeseries
        return position_ts.swaplevel(0, 1, axis=1)\
            .sort_index(axis=0)\
            .sort_index(axis=1, level=0, sort_remaining=False)

    @property  # @handler(False)
    def total_balance(self, time = None):
        if time is not None:
            # BLOCKING: make sure we have the latest quotes
            for asset in chain(self.orderbook.assets, self.portfolio.assets):
                self.fire(TickMarketDataClock(asset, time))

        with self.lock:
            return self.cash_balance + self.portfolio.total_position_value

