from datetime import datetime, timedelta
from functools import partial
from unittest import TestCase

import numpy as np
from sqlalchemy import create_engine

from testutils.database import get_sqlite_engine
from testutils.mocks import MockActor
from tradeengine.actors.orderbook_actor import order_sorter
from tradeengine.actors.sql.sql_orderbook import SQLOrderbookActor
from tradeengine.actors.sql.sql_portfolio import SQLPortfolioActor
from tradeengine.dto.dataflow import Asset, OrderTypes, QuantityOrder, CloseOrder, PercentOrder, PositionValue, \
    PortfolioValue, ExpectedExecutionPrice

AAPL = Asset("AAPL")


class TestOrderBookActors(TestCase):

    def test_orderbook_sort(self):
        time = datetime.now()
        sorter = partial(order_sorter, pv=PortfolioValue(100, {None: PositionValue(None, 1, 0, 0)}), expected_price=ExpectedExecutionPrice(time, 1,1,1,1))

        orders = [PercentOrder(None, 0.12, time, id=3), QuantityOrder(None, 10, time, id=2), QuantityOrder(None, -10, time, id=1), CloseOrder(None, None, time, id=0)]
        orders_sorted = list(sorted(orders, key=sorter))
        print(orders_sorted)
        self.assertListEqual([o.id for o in orders_sorted], list(range(4)))

        orders = [PercentOrder(None, 0.12, time, id=0), CloseOrder(None, None, datetime.now(), id=1)]
        self.assertListEqual([o.id for o in sorted(orders, key=sorter)], list(range(2)))

    def test_order_book_eviction(self):
        ob = SQLOrderbookActor(None, create_engine('sqlite://', echo=True))
        ob.place_order(QuantityOrder(AAPL, 12, datetime.now()))

        orders = ob.get_full_orderbook()
        print(orders)
        self.assertEquals(len(orders), 1)

        executable_orders = ob._get_orders_for_execution(AAPL, datetime.now(), 2, 2, 2, 2, 2, 2)
        print(executable_orders)
        self.assertEquals(len(executable_orders), 1)

        ob._evict_orders(AAPL, datetime.now() + timedelta(days=1))
        orders_after_eviction = ob.get_full_orderbook()
        print(orders_after_eviction)
        self.assertEquals(len(orders_after_eviction), 0)

        ob.on_stop()

    def test_market_order(self):
        pass

    def test_limit_order(self):
        pass

    def test_multiple_orders(self):
        engine = get_sqlite_engine(False)
        pa = MockActor(return_func=lambda x: PortfolioValue(100, {}))
        ob = SQLOrderbookActor(pa, engine)

        time = datetime.now()
        ob.place_order(PercentOrder(AAPL, 1, time))
        print("   1. ", ob.get_full_orderbook())
        self.assertEquals(ob.new_market_data(AAPL, datetime.now() - timedelta(seconds=10), 10, 10, 10, 10, 10, 10), 0)
        self.assertEquals(ob.new_market_data(AAPL, datetime.now(), 10, 10, 10, 10, 10, 10), 1)

        pa.return_func = lambda x: PortfolioValue(0, {AAPL: PositionValue(AAPL, 10, 1, 100)})
        ob.place_order(CloseOrder(AAPL, None, datetime.now()))
        print("   2. ", ob.get_full_orderbook())
        self.assertEquals(ob.new_market_data(AAPL, datetime.now(), 11, 11, 11, 11, 11, 11), 1)

        ob.place_order(PercentOrder(AAPL, 1, datetime.now()))
        print("   3. ", ob.get_full_orderbook())



