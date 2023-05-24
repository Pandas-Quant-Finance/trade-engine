from datetime import datetime
from unittest import TestCase

import pandas as pd

from tradeengine._obsolete.components.portfolio import Portfolio
from tradeengine._obsolete.events import Quote, Asset, TradeExecution

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
        ts = pf.get_timeseries()

        self.assertAlmostEqual(112 * 10, pf.total_position_value)
        self.assertListEqual([0, -100, -110, -120], ts["AAPL", "unrealized_pnl"].tolist())



