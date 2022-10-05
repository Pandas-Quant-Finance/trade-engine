from unittest import TestCase

import pandas as pd

from tradeengine import YFinanceBacktestingTradeEngine
from datetime import datetime


class TestYFinanceBacktestingTradeEngine(TestCase):

    def test_trade(self):
        te = YFinanceBacktestingTradeEngine()

        # long
        te.trade("AAPL", 10, timestamp=datetime.fromisoformat('2020-01-01'))  # buy
        te.trade("AAPL", 11, limit=72.98, timestamp=datetime.fromisoformat('2020-01-06'))  # buy limit
        te.trade("AAPL", 12, limit=75.60, timestamp=datetime.fromisoformat('2020-01-09'))  # buy limit, not executed

        # close
        te.trade("AAPL", -12, timestamp=datetime.fromisoformat('2020-01-12'))  # sell
        te.trade("AAPL", -9, limit=77.88, timestamp=datetime.fromisoformat('2020-01-13'))  # sell limit
        te.trade("AAPL", -5, limit=77.95, timestamp=datetime.fromisoformat('2020-01-13'))  # sell limit not executed

        # swing
        te.trade("AAPL", -6, timestamp=datetime.fromisoformat('2020-01-15'))  # sell
        te.trade("AAPL", 12, timestamp=datetime.fromisoformat('2020-01-17'))  # buy
        te.trade("AAPL", -6, timestamp=datetime.fromisoformat('2020-01-22'))  # sell

        # same day, throws exception
        with self.assertRaises(RecursionError):
            te.trade("AAPL", 10, timestamp=datetime.fromisoformat('2020-01-24'))  # buy
            te.trade("AAPL", -10, timestamp=datetime.fromisoformat('2020-01-24'))  # sell

        # just for grouping purpose
        te.trade("MSFT", 12, timestamp=datetime.fromisoformat('2020-01-17'))  # buy
        te.trade("MSFT", -12, timestamp=datetime.fromisoformat('2020-01-22'))  # sell

        print("\n", te.positions)
        print("\n", te.get_history(cash=10000))

        te.get_history(cash=10000).to_csv("pnl.csv")
        # also test limit and position id (to long short same asset)
        pass

    def test_close(self):
        te = YFinanceBacktestingTradeEngine()
        te.trade("AAPL", 10, timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Long")  # long
        te.trade("AAPL", -10, timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Short")  # short
        te.close('AAPL', timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")  # close long
        te.close('AAPL', timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Short")  # close short

        hist = te.get_history(cash=10000)
        #hist.to_csv("pnl2.csv")
        #print("\n", hist)

        self.assertLessEqual(hist["TOTAL", "pnl"].max(), 0.00001)

    def test_target_weights(self):
        with self.assertRaises(AssertionError):
            te = YFinanceBacktestingTradeEngine()
            te.target_weights(["AAPL", "MSFT"], [0.25, 0.75], timestamp=datetime.fromisoformat('2020-01-01'))

        with self.assertRaises(AssertionError):
            te = YFinanceBacktestingTradeEngine(start_capital=1000)
            te.target_weights(["AAPL", "MSFT"], [0.25, 0.74], timestamp=datetime.fromisoformat('2020-01-01'))

        te = YFinanceBacktestingTradeEngine(start_capital=1000)
        te.target_weights(["AAPL", "MSFT"], [0.25, 0.75], timestamp=datetime.fromisoformat('2020-01-01'))
        te.target_weights(["AAPL", "MSFT"], [0.75, 0.25], timestamp=datetime.fromisoformat('2020-01-06'))
        te.target_weights(["AMZN", "MSFT"], [0.10, 0.90], timestamp=datetime.fromisoformat('2020-01-07'))

        print("\n", te.positions, "\n", te.start_capital)

        hist = te.get_history()
        print("\n", hist)

    def test_target_weights_1_over_n(self):
        from common.pandas_extensions import cumpct_change
        te = YFinanceBacktestingTradeEngine(start_capital=1000)
        pd.date_range('2020-01-01', '2020-12-31').to_series().apply(
            lambda x: te.target_weights(["AAPL", "MSFT"], [0.5, 0.5], timestamp=x.to_pydatetime())
        )

        hist = te.get_history()

        self.assertLessEqual(
            hist[("MSFT", "Close")].cumpct_change().iloc[-1],
            hist[("TOTAL", "pnl_percent")].iloc[-1]
        )

        self.assertGreaterEqual(
            hist[("AAPL", "Close")].cumpct_change().iloc[-1],
            hist[("TOTAL", "pnl_percent")].iloc[-1]
        )
