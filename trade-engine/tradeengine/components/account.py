from __future__ import annotations
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import pandas as pd
from circuits import Component, handler
from ..events import *

_log = logging.getLogger(__name__)


class Account(Component):

    def __init__(self, starting_balance: float = 100):
        super().__init__()
        self._starting_balance = starting_balance
        self.cash_balance = self._starting_balance
        self.position_value = 0
        self.positions: Dict[Asset, Dict[str, Position]] = {}
        self.orderbook: Dict[Asset, List[Order]] = defaultdict(list)
        self.position_timeseries: Dict[str, Dict[datetime, PositionTimeSeries]] = defaultdict(dict)

    def target_weights(self):
        # _ = yield self.fire(ReadyForComplexTradeEvent([order.asset], order.valid_from))

        pass

    @handler(SubmitTradeEvent.__name__)
    def trade(self, order: Order):
        _log.debug(f"got new order {order}")

        # put order onto order book
        if abs(order.quantity) > 1e-4:
            self.orderbook[order.asset].append(order)

        _ = yield self.fire(SubscribeToQuoteProviderEvent(order.asset, order.valid_from))

    @handler(TradeExecutedEvent.__name__)
    def handle_trades(self, trade: TradeExecution):
        _log.debug(f"got new trade execution {trade}")
        self.cash_balance += (-trade.quantity * trade.price)

        if trade.asset in self.positions:
            if trade.position_id in self.positions[trade.asset]:
                self.positions[trade.asset][trade.position_id] += trade.quantity
            else:
                self.positions[trade.asset][trade.position_id] = Position(trade.position_id, trade.asset,
                                                                          trade.quantity)
        else:
            self.positions[trade.asset] = {trade.position_id: Position(trade.position_id, trade.asset, trade.quantity)}

    @handler(QuoteUpdatedEvent.__name__)
    def quote_update(self, quote: Quote):
        _log.debug(f"got new quote {quote}")
        # TODO check if quote.price is float | BidAsk | Bar

        # check order book if order was triggered
        obsolete_orders = set()
        for i, o in enumerate(self.orderbook[quote.asset]):
            if quote.time > o.valid_from:
                if (o.limit is None) \
                or (o.quantity > 0 and quote.price <= o.limit) \
                or (o.quantity < 0 and quote.price >= o.limit):
                    # execute trade and wait for event complete
                    _ = yield self.call(TradeExecutedEvent(TradeExecution(o.asset, o.quantity, quote.price, o.position_id)))
                    obsolete_orders.add(i)
                    print(self.positions)
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
                if abs(pos.quantity) < 1e-6:
                    obsolete_pids.append(pid)
                else:
                    self.position_value += (pos.quantity * quote.price)
                    self.position_timeseries[pid][quote.time] = PositionTimeSeries(
                        pid, quote.time, pos.quantity, quote.price
                    )

            for opid in obsolete_pids:
                print("remove pid", opid)
                del self.positions[quote.asset][opid]

    def get_history(self):
        return pd.concat(
            [pd.DataFrame(ts) for ts in self.position_timeseries.values()],
            axis=1,
            keys=self.position_timeseries.keys(),
        )
