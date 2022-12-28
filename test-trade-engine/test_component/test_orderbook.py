from unittest import TestCase

from tradeengine.components.orderbook import OrderBook
from tradeengine.events import Order, Quote, Asset, TradeExecution


class TestOrderBook(TestCase):

    def test_order_execution(self):
        execution_cnt = [0]
        def handler(te):
            execution_cnt[0] += 1

        ob = OrderBook()
        ob.register(TradeExecution, handler=handler)

        ob.place_order(Order("AAPL", 10, 100, valid_from='2022-01-08'))
        ob.place_order(Order("AAPL", 10, 98, valid_from='2022-01-08', valid_to=3, position_id='A'))
        ob.place_order(Order("AAPL", 10, 95, valid_from='2022-01-08', valid_to=3, position_id='A'))

        ob.on_quote_update(Quote("AAPL", '2022-01-08', 110))
        self.assertEqual(0, execution_cnt[0])
        ob.on_quote_update(Quote("AAPL", '2022-01-09', 110))
        self.assertEqual(0, execution_cnt[0])
        ob.on_quote_update(Quote("AAPL", '2022-01-10', 99))
        self.assertEqual(1, execution_cnt[0])
        ob.on_quote_update(Quote("AAPL", '2022-01-11', 98))
        self.assertEqual(2, execution_cnt[0])
        self.assertEqual(0, len(ob.orderbook[Asset("AAPL")]))


    def test_order_expiration(self):
        ob = OrderBook()
        ob.place_order(Order("AAPL", 10, 100, valid_from='2022-01-08'))
        ob.place_order(Order("AAPL", 10, 100, valid_from='2022-01-08', valid_to=2, position_id='A'))
        ob.place_order(Order("AAPL", 10, 100, valid_from='2022-01-08', valid_to='2022-01-11', position_id='B'))

        self.assertEqual(1, len(ob.orderbook))
        self.assertEqual(3, len(ob.orderbook[Asset("AAPL")]))

        ob.on_quote_update(Quote("AAPL", '2022-01-08', 110))
        ob.on_quote_update(Quote("AAPL", '2022-01-09', 110))
        ob.on_quote_update(Quote("AAPL", '2022-01-10', 110))
        self.assertEqual(2, len(ob.orderbook[Asset("AAPL")]))

        ob.on_quote_update(Quote("AAPL", '2022-01-11', 110))
        self.assertEqual(1, len(ob.orderbook[Asset("AAPL")]))

        ob.on_quote_update(Quote("AAPL", '2022-01-12', 110))
        self.assertEqual(1, len(ob.orderbook[Asset("AAPL")]))

        ob.cancel_order(ob.orderbook[Asset("AAPL")][0])
        self.assertEqual(0, len(ob.orderbook[Asset("AAPL")]))