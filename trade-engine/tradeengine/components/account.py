from __future__ import annotations
import logging
from abc import abstractmethod
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from typing import Dict, List, Any, Tuple

import pandas as pd

from ..events import *

_log = logging.getLogger(__name__)


class Account(object):

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


def check_limit(quantity: float, quote: Quote, limit: float):
    pricing = quote.price
    if isinstance(pricing, float):
        if quantity > 0 and pricing <= limit:
            return True
        if quantity < 0 and pricing >= limit:
            return True
    elif isinstance(pricing, BidAsk):
        if quantity > 0 and pricing.ask <= limit:
            return True
        if quantity < 0 and pricing.bid >= limit:
            return True
    elif isinstance(pricing, Bar):
        if quantity > 0:
            return limit < pricing.high
        if quantity < 0:
            return limit > pricing.low
    else:
        raise ValueError(f"Unknown quoting {type(pricing)}")

    return False


def get_price(quantity: float, quote: Quote, limit: float | str = None, slippage: float = 0):
    pricing = quote.price
    slippage_factor = (1 + slippage) if quantity > 0 else (1 - slippage)

    if isinstance(pricing, float):
        return pricing
    elif isinstance(pricing, BidAsk):
        if quantity > 0:
            return pricing.ask * slippage_factor
        elif quantity < 0:
            return pricing.bid * slippage_factor
        else:
            return (pricing.bid + pricing.ask) / 2  * slippage_factor
    elif isinstance(pricing, Bar):
        if limit is None:
            return pricing.open * slippage_factor
        elif quantity == 0:
            return pricing.close * slippage_factor
        elif limit == 'last':
            return pricing.close * slippage_factor
        else:
            return limit * slippage_factor
    else:
        raise ValueError(f"Unknown quoting {type(pricing)}")

