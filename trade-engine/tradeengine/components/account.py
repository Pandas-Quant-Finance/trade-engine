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
        self.cash_timeseries: Dict[datetime, float] = {}
        self.quotes: Dict[Asset, Quote] = {}

        self.register(TradeExecution, handler=self.on_trade_execution)
        self.register(TargetWeights, handler=self.place_target_weights_oder)
        self.register(MaximumOrder, handler=self.place_maximum_order)

    def on_quote_update(self, quote: Quote):
        with self.lock:
            if quote.asset in self.quotes:
                if timestamp_greater_equal(quote.time, self.quotes[quote.asset].time):
                    self.quotes[quote.asset] = quote
            else:
                self.quotes[quote.asset] = quote

    def on_trade_execution(self, trade: TradeExecution):
        with self.lock:
            self.cash_balance += (-trade.quantity * trade.price)
            self.cash_timeseries[trade.time] = self.cash_balance

    def place_maximum_order(self, order: MaximumOrder):
        # make sure we have the latest market data
        self.fire(TickMarketDataClock(order.asset, order.valid_from))

        # get maximum possible capital and place order
        balance = self.cash_balance * (1 - self.derive_quantity_slippage)

        # use derive_quantity_slippage to allow market movements from this tick to the next one
        with self.lock:
            price = self.quotes[order.asset].get_price(order.quantity, 'last')

        self.fire(Order(order.asset, balance / price, order.limit, order.valid_from, order.valid_to, order.position_id))

    def place_target_weights_oder(self, target_weights: TargetWeights, slippage: float = 0.02, min_weight=1e-3):
        # make sure we have the latest market data
        for asset in target_weights.asset_weights.keys():
            self.fire(TickMarketDataClock(asset, target_weights.valid_from))

        # TODO calculate the quantity from the weights
        #  find the difference between the current position quantity and the target quantity
        #  place orders of quantity if the quantity is not negligible small
        #  use derive_quantity_slippage
        #  use with self.lock: for self.quotes access
        pass

    def get_current_weights(self, time: datetime | str, pid: str = '') -> Dict[Asset, float]:
        # we need all the latest quotes such that we can calculate the current weights
        for asset in chain(self.portfolio.assets, self.orderbook.assets):
            self.fire(TickMarketDataClock(asset, time))

        with self.lock:
            return self.portfolio.get_weights(self.cash_balance)

    def get_history(self):
        # FIXME ...
        return pd.concat(
            [pd.DataFrame(
                [pd.Series(asdict(x), name=t) for t, x in ts.items()]
            ) for ts in self.position_timeseries.values()],
            axis=1,
            keys=self.position_timeseries.keys()
        ) if len(self.position_timeseries) > 0 else pd.DataFrame({})

    @property
    def total_balance(self, time = None):
        if time is not None:
            for asset in chain(self.orderbook.assets, self.portfolio.assets):
                self.fire(TickMarketDataClock(asset, time))

        with self.lock:
            return self.cash_balance + self.portfolio.total_position_value



class AccountOLD(object):

    def __init__(self, starting_balance: float = 100, slippage: float = 0.002):
        super().__init__()
        self._starting_balance = starting_balance
        self.cash_balance = self._starting_balance
        self.slippage = slippage
        self.position_value: Dict[str, float] = defaultdict(lambda: 0)
        self.positions: Dict[Asset, Dict[str, Position]] = {}
        self.orderbook: Dict[Asset, List[Order]] = defaultdict(list)
        self.position_timeseries: Dict[str, Dict[datetime, PositionTimeSeries]] = defaultdict(dict)
        self.quote: Dict[Asset, float] = defaultdict(lambda: 0)
        self.quote_date: datetime = None

    def target_weights(self, target_weights: TargetWeights, slippage: float = 0.02, min_weight=1e-3):
        self.prepare_to_trade(target_weights.asset_weights[0], target_weights.valid_from)
        balance = self.total_balance * (1 - slippage)
        stocks, quantity = [], []

        for a, w in zip(*target_weights.asset_weights):
            if w < min_weight: continue

            # calculate stocks from weights
            try:
                target_quantity = (w * balance) / self.quote[a]
            except ZeroDivisionError as zde:
                raise ValueError(f"No quote for {a} before {target_weights.valid_from}", zde)

            # get delta quantity
            delta_quantity = target_quantity
            pid = (str(target_weights.position_id) or '') + '/' + str(a.id)
            if a in self.positions:
                if pid in self.positions[a]:
                    delta_quantity -= self.positions[a][pid].quantity

            if delta_quantity > 1e-4:
                stocks.append(a)
                quantity.append(delta_quantity)

        #  create trades
        trades = Order(
            stocks,
            quantity,
            valid_from=target_weights.valid_from,
            valid_to=target_weights.valid_to,
            position_id=target_weights.position_id
        )
        self._trade(trades)

    def trade(self, order: Order):
        if abs(order.quantity) > 1e-4:
            self.prepare_to_trade(
                order.asset if isinstance(order.asset, list) else [order.asset],
                order.valid_from
            )
            self._trade(order)

    def _trade(self, order: Order):
        _log.debug(f"got new order {order}")

        # put order onto order book
        if isinstance(order.asset, list):
            for i in range(len(order.asset)):
                o = order.order_at_index(i)
                if abs(o.quantity) > 1e-4:
                    self.orderbook[o.asset].append(o)
                    self.subscribe_market_data(o.asset, o.valid_from)
        else:
            if abs(order.quantity) > 1e-4:
                self.orderbook[order.asset].append(order)
                self.subscribe_market_data(order.asset, order.valid_from)

    def handle_trade_execution(self, trade: TradeExecution):
        _log.debug(f"got new trade execution {trade}")
        self.cash_balance += (-trade.quantity * trade.price)

        if trade.asset in self.positions:
            if trade.position_id in self.positions[trade.asset]:
                self.positions[trade.asset][trade.position_id] += trade.quantity
            else:
                self.positions[trade.asset][trade.position_id] = \
                    Position(trade.position_id, trade.asset, trade.quantity)
        else:
            self.positions[trade.asset] = {trade.position_id: Position(trade.position_id, trade.asset, trade.quantity)}

    def quote_update(self, quote: Quote):
        _log.debug(f"got new quote {quote}")

        # remember quote
        self.quote[quote.asset] = get_price(0, quote, None)
        self.quote_date = quote.time

        # check order book if order was triggered
        obsolete_orders = set()
        for i, o in enumerate(self.orderbook[quote.asset]):
            o = o.tz_aware(quote.time)
            if quote.time > o.valid_from:
                if (o.limit is None) \
                or check_limit(o.quantity, quote, o.limit):
                    # execute trade and wait for event complete
                    price = get_price(o.quantity, quote, o.limit, self.slippage)
                    self.handle_trade_execution(TradeExecution(o.asset, o.quantity, price, o.position_id))
                    obsolete_orders.add(i)
            elif o.valid_to is not None:
                if isinstance(o.valid_to, datetime) and o.valid_to < quote.time:
                    obsolete_orders.add(i)
                else:
                    self.orderbook[quote.asset][i] = o - 1
                    if self.orderbook[quote.asset][i].valid_to <= 0:
                        obsolete_orders.add(i)

        # delete obsolete orders
        self.orderbook[quote.asset] = [o for i, o in enumerate(self.orderbook[quote.asset]) if i not in obsolete_orders]

        # update all position market values (append current position to a time series)
        if quote.asset in self.positions:
            obsolete_pids = []
            for pid, pos in self.positions[quote.asset].items():
                price = get_price(pos.quantity, quote, 'last')
                self.position_value[pid] = (pos.quantity * price)
                self.position_timeseries[pid][quote.time] = PositionTimeSeries(
                    pid, quote.time, pos.quantity, price
                )

                if abs(pos.quantity) < 1e-6:
                    obsolete_pids.append(pid)

            for opid in obsolete_pids:
                print("remove pid", opid)
                del self.positions[quote.asset][opid]

        # append cash time series
        self.position_timeseries["$"][quote.time] = PositionTimeSeries(
            "$", quote.time, self.cash_balance, 1.0
        )

    def get_current_weights(self, time: datetime | str, pid: str = '') -> Dict[Asset, float]:
        self.prepare_to_trade(
            list(self.positions.keys()) + list(self.orderbook.keys()),
            datetime.fromisoformat(time) if isinstance(time, str) else time
        )

        balance = self.total_balance
        weights = {}

        for a, positions in self.positions.items():
            pos = positions[pid + "/" + str(a.id)]
            weights[a] = pos.quantity * self.quote[a] / balance

        return weights

    def get_history(self):
        self.prepare_for_summary()
        return pd.concat(
            [pd.DataFrame(
                [pd.Series(asdict(x), name=t) for t, x in ts.items()]
            ) for ts in self.position_timeseries.values()],
            axis=1,
            keys=self.position_timeseries.keys()
        ) if len(self.position_timeseries) > 0 else pd.DataFrame({})

    @property
    def position_value_balance(self):
        return sum(self.position_value.values())

    @property
    def total_balance(self):
        return self.cash_balance + self.position_value_balance

    @abstractmethod
    def subscribe_market_data(self, asset: Asset, valid_from: datetime):
        pass

    @abstractmethod
    def prepare_to_trade(self, assets: List[Asset], time: datetime):
        pass

    @abstractmethod
    def prepare_for_summary(self):
        pass
