from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from threading import Lock
from typing import Dict, List

from tradeengine.common.tz_compare import timestamp_greater, timestamp_greater_equal
from tradeengine.events import Asset, Order, Quote, TradeExecution
from tradeengine.events.data import BasketOrder, SubscribeToMarketData, CancelOrder
from .component import Component


class OrderBook(Component):

    def __init__(self, slippage: float = 0, min_quantity: float = 1e-4):
        super().__init__()
        self.min_quantity = min_quantity
        self.orderbook: Dict[Asset, List[Order]] = defaultdict(list)
        self.slippage = slippage

        self.lock = Lock()

        self.register(Quote, handler=self.on_quote_update)
        self.register(Order, BasketOrder, handler=self.place_order)
        self.register(CancelOrder, handler=self.cancel_order)

    def on_quote_update(self, quote: Quote):
        # check order book if order was triggered
        with self.lock:
            # get a copy of open orders for this asset
            orders_4_asset = tuple(self.orderbook[quote.asset])

            for o in orders_4_asset:
                if timestamp_greater(quote.time, o.valid_from):
                    execution_price = quote.get_price(o.quantity, o.limit, self.slippage)
                    if execution_price is not None:
                        # BLOCKING: execute trade and wait for event complete
                        self.fire(TradeExecution(o.asset, o.quantity, execution_price, quote.time, quote, o.position_id))
                        self._remove(o)
                    else:
                        if o.valid_to is not None:
                            if isinstance(o.valid_to, datetime):
                                if timestamp_greater_equal(quote.time, o.valid_to):
                                    self._remove(o)
                            else:
                                if not o.valid_after_subtract_tick():
                                    self._remove(o)

    def place_order(self, order: Order | BasketOrder):
        if isinstance(order, BasketOrder):
            for o in order.orders:
                self.place_order(o)
        else:
            self.fire(SubscribeToMarketData(order.asset, order.valid_from))
            with self.lock:
                if abs(order.quantity) > self.min_quantity:
                    self.orderbook[order.asset].append(order)

        return self

    def cancel_order(self, order: Order | CancelOrder):
        with self.lock:
            return self._remove(order.order if isinstance(order, CancelOrder) else order)

    def _remove(self, order: Order):
        self.orderbook[order.asset].remove(order)
        return self

    @property
    def assets(self):
        return list(self.orderbook.keys())

    def on_trade_execution(self, trade_execution: TradeExecution):
        pass
