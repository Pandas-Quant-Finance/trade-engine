from datetime import datetime
from unittest import TestCase

import pandas as pd

from tradeengine.components.orderbook import OrderBook
from tradeengine.components.portfolio import Portfolio
from tradeengine.events import Order, Quote, Asset, TradeExecution

# show all columns
pd.set_option('display.max_columns', None)


class TestPortfolio(TestCase):

    def test_quote_update(self):
        pf = Portfolio()

        pf.on_trade_execution(TradeExecution(Asset("AAPL"), 10, 100, datetime.fromisoformat('2020-01-01'), Quote("AAPL", None, 100)))
        pf.on_quote_update(Quote("MSFT", '2020-01-02', 100))
        pf.on_quote_update(Quote("AAPL", '2020-01-02', 110))
        pf.on_quote_update(Quote("AAPL", '2020-01-03', 111))
        pf.on_quote_update(Quote("AAPL", '2020-01-04', 112))
        pf.on_quote_update(Quote("AAPL", '2020-01-03', 111))

        self.assertAlmostEqual(1120, pf.total_position_value)
        self.assertAlmostEqual(112, pf.latest_quote[Asset("AAPL")][1])

        print(pf.get_timeseries())


