from unittest import TestCase

import numpy as np
import pandas as pd

from tradeengine import YFinanceBacktestingTradeEngine
from datetime import datetime

from tradeengine._obsolete.components import YfBacktester
from tradeengine._obsolete.events import *


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

    def test_trade_max(self):
        te = YFinanceBacktestingTradeEngine(start_capital=100)
        _, q, _ = te.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Long")

        self.assertEqual(
            te.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-09-01'), position_id="APPL-Long"),
            ('APPL-Long', 0, 0)
        )
        te.trade("AAPL", -q / 2, timestamp=datetime.fromisoformat('2020-10-02'), position_id="APPL-Long"),

        self.assertGreater(
            te.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-10-05'), position_id="APPL-Long")[1],
            0.5
        )
        self.assertEqual(
            te.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-10-06'), position_id="APPL-Long"),
            ('APPL-Long', 0, 0)
        )

    def test_trade_max_with_slippage(self):
        te = YFinanceBacktestingTradeEngine(start_capital=100)
        _, q, _ = te.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-01-01'), slippage=0.01, position_id="APPL-Long")

        self.assertAlmostEqual(
            te.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-10-06'), slippage=0.01,
                     position_id="APPL-Long")[1],
            0
        )
        te.trade("AAPL", -q / 2, timestamp=datetime.fromisoformat('2020-10-02'), slippage=0.01, position_id="APPL-Long"),

        self.assertGreater(
            te.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-10-05'), slippage=0.01, position_id="APPL-Long")[1],
            0.5
        )
        self.assertAlmostEqual(
            te.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-10-06'), slippage=0.01, position_id="APPL-Long")[1],
            0
        )

    def test_pnl(self):
        te = YFinanceBacktestingTradeEngine()
        te.trade("AAPL", 10, timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Long")
        te.close('AAPL', timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")
        te.trade("AAPL", 10, timestamp=datetime.fromisoformat('2020-09-01'), position_id="APPL-Long")
        te.close('AAPL', timestamp=datetime.fromisoformat('2020-09-06'), position_id="APPL-Long")

        hist_no_cash = te.get_history()
        hist_cash = te.get_history(cash=5000)

        print("")

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
            te.target_weights(["AAPL", "MSFT"], [0.25, 0.76], timestamp=datetime.fromisoformat('2020-01-01'))

        te = YFinanceBacktestingTradeEngine(start_capital=1000)
        te.target_weights(["AAPL", "MSFT"], [0.25, 0.75], timestamp=datetime.fromisoformat('2020-01-01'))
        te.target_weights(["AAPL", "MSFT"], [0.75, 0.25], timestamp=datetime.fromisoformat('2020-01-06'))
        te.target_weights(["AMZN", "MSFT"], [0.10, 0.90], timestamp=datetime.fromisoformat('2020-01-07'))

        print("\n", te.positions, "\n", te.start_capital)

        hist = te.get_history()
        print("\n", hist)

    def test_target_weights_1_over_n(self):
        te = YFinanceBacktestingTradeEngine(start_capital=1000)
        pd.date_range('2020-01-01', '2020-12-31').to_series().apply(
            lambda x: te.target_weights(["AAPL", "MSFT"], [0.5, 0.5], timestamp=x.to_pydatetime())
        )

        hist = te.get_history()
        min_ret = min(hist[("MSFT", "Close")].cumpct_change().iloc[-1],hist[("AAPL", "Close")].cumpct_change().iloc[-1])
        max_ret = max(hist[("MSFT", "Close")].cumpct_change().iloc[-1],hist[("AAPL", "Close")].cumpct_change().iloc[-1])

        self.assertLessEqual(
            hist[("TOTAL", "pnl_percent")].iloc[-1],
            max_ret
        )

        self.assertGreaterEqual(
            hist[("TOTAL", "pnl_percent")].iloc[-1],
            min_ret
        )

    def test_get_weights(self):
        te = YfBacktester('2020-01-01', 1000)
        te.target_weights(TargetWeights(([Asset("AAPL"), Asset("MSFT")], [0.25, 0.75]), valid_from=datetime.fromisoformat('2020-01-04')))
        cw = te.get_current_weights(datetime.fromisoformat('2020-01-06'))
        # print(cw)

        np.testing.assert_almost_equal(
            np.array([0.247, 0.739]),
            np.array(list(cw.values())),
            decimal=3
        )

    def test_odd_target_weights(self):
        te = YFinanceBacktestingTradeEngine(start_capital=1000)
        te.target_weights(["AAPL", "MSFT"], [0.2, 0.5], timestamp=datetime.fromisoformat('2020-01-01'))
        cw = te.get_current_weights(timestamp=datetime.fromisoformat('2020-01-02'))
        # print(cw)

        np.testing.assert_almost_equal(
            np.array([0.198, 0.4948]),
            np.array(list(cw.values())),
            decimal=3
        )

    def test_target_weights_with_shorts(self):
        te = YFinanceBacktestingTradeEngine(start_capital=1000)
        te.target_weights(["AAPL", "MSFT", "AMZN"], [0.2, 0.8, -1.0], timestamp=datetime.fromisoformat('2020-01-01'))
        cw = te.get_current_weights(timestamp=datetime.fromisoformat('2020-01-02'))
        # print(cw)

        np.testing.assert_almost_equal(
            np.array([0.200, -1.006, 0.798]),
            np.array(list(cw.values())),
            decimal=3
        )
