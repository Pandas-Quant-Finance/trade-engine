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
        self.latest_quotes: Dict[Asset, Quote] = {}    # TODO replace with LatestQuote

        self.register_event(TradeExecution, handler=self.on_trade_execution)
        self.register_event(TargetWeights, handler=self.place_target_weights_oder)
        self.register_event(MaximumOrder, handler=self.place_maximum_order)
        self.register_event(CloseOrder, handler=self.on_close_position)
        self.register_event(Quote, handler=self.on_quote_update)

        _log.info(
            "created account with ",
            {
                "starting_balance": starting_balance,
                "slippage": slippage,
                "derive_quantity_slippage": derive_quantity_slippage,
                "order_minimum_quantity": order_minimum_quantity,
                "min_target_weight": min_target_weight,
            }
        )

    # @handler(False)
    def place_all_orders(self, s: pd.Series, timit=True):
        start = datetime.now()
        order_count = 0

        for date, item in s.items():
            if isinstance(item, Order):
                self.place_order(item)
                if timit: order_count += 1
            elif isinstance(item, MaximumOrder):
                self.place_maximum_order(item)
                if timit: order_count += 1
            elif isinstance(item, TargetWeights):
                self.place_target_weights_oder(item)
                if timit: order_count += len(item.asset_weights)
            elif isinstance(item, CloseOrder):
                self.place_close_position_order(item)
                if timit: order_count += len(self.portfolio.positions[item.position]) if isinstance(item.position, Asset) else 1
            else:
                raise ValueError(f"Unknown order of type {type(item)}")

        if timit:
            _log.warning(f"placed {order_count} orders in {(datetime.now() - start).seconds} seconds")

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
                pid = target_weights.position_id + "/" + asset.id
                aw = abs(weight)

                if aw < self.min_target_weight:
                    target_quantity = 0
                else:
                    target_quantity = (total_balance * weight) / self.latest_quotes[asset].get_price(weight, 'last')

                current_quantity = self.portfolio.get_quantity(asset, pid)
                trade_quantity = target_quantity - current_quantity

                if abs(current_quantity) < 1e-10 and abs(trade_quantity) < 1e-10:
                    continue

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

        position_ts = position_ts.join(
            pd.concat(
                [
                    position_ts["value"].sum(axis=1).rename(("value", "TOTAL")),
                    position_ts["unrealized_pnl"].sum(axis=1).rename(("unrealized_pnl", "TOTAL")),
                    position_ts["realized_pnl"].sum(axis=1).rename(("realized_pnl", "TOTAL")),
                    position_ts["pnl"].sum(axis=1).rename(("pnl", "TOTAL")),
                ],
                axis=1
            )
        )

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

        # also add the cash balance to the total
        position_ts["balance", "TOTAL"] = position_ts["value", "TOTAL"] + position_ts['balance', '$CASH$']
        position_ts["pnl%", "TOTAL"] = position_ts["balance", "TOTAL"] * (1 / self._starting_balance) - 1
        position_ts["return", "TOTAL"] = position_ts["balance", "TOTAL"].pct_change()
        position_ts["return", "TOTAL"].iloc[1] = \
            position_ts["balance", "TOTAL"].iloc[1] / self._starting_balance - 1

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

