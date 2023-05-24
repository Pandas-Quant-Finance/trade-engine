from datetime import datetime
from unittest import TestCase

from tradeengine._obsolete.events import Quote, BidAsk, Bar


class TestQuoteData(TestCase):

    def test_price_limit(self):
        q = Quote("AAPL", datetime.now(), 10.0)

        # slippage
        self.assertAlmostEqual(11, q.get_price(1, None, 0.1))
        self.assertAlmostEqual(9, q.get_price(-1, None, 0.1))

        # limit
        self.assertAlmostEqual(10, q.get_price(-1, 9))
        self.assertIsNone(q.get_price(1, 9))
        self.assertAlmostEqual(10, q.get_price(1, 11))
        self.assertIsNone(q.get_price(-1, 11))

    def test_bid_ask_limit(self):
        q = Quote("AAPL", datetime.now(), BidAsk(10, 11))

        # slippage
        self.assertAlmostEqual(12.1, q.get_price(1, None, 0.1))
        self.assertAlmostEqual(9, q.get_price(-1, None, 0.1))

        # limit
        self.assertAlmostEqual(10, q.get_price(-1, 9))
        self.assertIsNone(q.get_price(1, 9))
        self.assertAlmostEqual(11, q.get_price(1, 12))
        self.assertIsNone(q.get_price(-1, 11))

    def test_bar_limit(self):
        q = Quote("AAPL", datetime.now(), Bar(10, 12, 9, 11))

        # slippage
        self.assertAlmostEqual(11, q.get_price(1, None, 0.1))
        self.assertAlmostEqual(9, q.get_price(-1, None, 0.1))

        # limit
        self.assertAlmostEqual(10, q.get_price(-1, 9))
        self.assertAlmostEqual(11, q.get_price(-1, 11))
        self.assertIsNone(q.get_price(-1, 13))

        self.assertIsNone(q.get_price(1, 8))
        self.assertAlmostEqual(10, q.get_price(1, 13))
        self.assertAlmostEqual(10, q.get_price(1, 10))







