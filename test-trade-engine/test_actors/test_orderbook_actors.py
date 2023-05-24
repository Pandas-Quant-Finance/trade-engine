from datetime import datetime, timedelta
from functools import partial
from unittest import TestCase

import numpy as np
from sqlalchemy import create_engine

from tradeengine.actors.orderbook_actor import order_sorter
from tradeengine.actors.sql.orderbook import SQLOrderbookActor
from tradeengine.actors.sql.portfolio import SQLPortfolioActor
from tradeengine.dto.dataflow import Asset, OrderTypes, QuantityOrder, CloseOrder, PercentOrder, PositionValue, \
    PortfolioValue


class TestOrderBookActors(TestCase):

    def test_orderbook_sort(self):
        orders = [PercentOrder(None, 0.12, None, id=3), QuantityOrder(None, 10, None, id=2), QuantityOrder(None, -10, None, id=1), CloseOrder(None, None, None, id=0)]
        orders_sorted = list(sorted([(o, 1) for o in orders], key=partial(order_sorter, pv=PortfolioValue(100, {None: PositionValue(None, 1, 0, 0)}))))
        print(orders_sorted)
        self.assertListEqual([o.id for o, _ in orders_sorted], list(range(4)))

    def test_order_book(self):
        ob = SQLOrderbookActor(None, create_engine('sqlite://', echo=True))
        ob.place_order(QuantityOrder(Asset("AAPL"), 12, datetime.now()))

        orders = ob.get_full_orderbook()
        print(orders)
        self.assertEquals(len(orders), 1)

        executable_orders = ob._get_orders_for_execution(Asset("AAPL"), datetime.now(), 2, 2, 2, 2, 2, 2)
        print(executable_orders)
        self.assertEquals(len(executable_orders), 1)

        ob._evict_orders(Asset("AAPL"), datetime.now() + timedelta(days=1))
        orders_after_eviction = ob.get_full_orderbook()
        print(orders_after_eviction)
        self.assertEquals(len(orders_after_eviction), 0)

        ob.on_stop()
