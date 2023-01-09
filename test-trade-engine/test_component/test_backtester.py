import datetime
from unittest import TestCase

import numpy as np
import pandas as pd
import os

import pytest

from tradeengine.components.backtester import PandasBarBacktester
from tradeengine.components.component import Component
from tradeengine.events import Bar, MaximumOrder, CloseOrder, Order, TargetWeights, Asset

# show all columns
pd.set_option('display.max_columns', None)

path = os.path.dirname(os.path.abspath(__file__))
df_aapl = pd.read_csv(f"{path}/../aapl.csv", index_col="Date", parse_dates=True)['2022-01-01':]
df_msft = pd.read_csv(f"{path}/../msft.csv", index_col="Date", parse_dates=True)['2022-01-01':]


class TestBackTester(TestCase):

    def test_simple(self):
        Component().get_handlers().clear()
        bt = PandasBarBacktester(
            lambda a, x: pd.read_csv(f"{path}/../{a.id.lower()}.csv", index_col="Date", parse_dates=True)[x:],
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            '2022-01-01',
            100
        )

        bt.place_maximum_order(MaximumOrder("AAPL", valid_from='2022-01-03'))
        bt.place_close_position_order(CloseOrder(None, valid_from='2022-01-28'))
        bt.place_close_position_order(CloseOrder(Asset("MSFT"), valid_from='2022-01-28'))

        # the pnl should be very close to just the move of the stock
        buy_and_hold = ((df_aapl.loc[:"2022-02-01", "Close"].pct_change().fillna(0) + 1).cumprod() - 1)
        #print(df.iloc[0, 3], df.iloc[-1, 3])

        dfhist = bt.get_history().dropna()
        print(dfhist.dropna())

        # 181.8 vs 169.45
        self.assertAlmostEqual(-0.06714, dfhist["AAPL", "pnl_%"].iloc[-1], 5)
        self.assertAlmostEqual(-0.06714, dfhist.dropna()["TOTAL", "pnl_%"].iloc[-1], 5)
        self.assertLess(dfhist["TOTAL", "pnl_%"].iloc[-1], buy_and_hold.iloc[-1])
        self.assertLess(dfhist["AAPL", "pnl_%"].iloc[-1], buy_and_hold.iloc[-1])

        # no stupid closing order hanging
        self.assertNotIn(Asset("MSFT"), bt.orderbook.orderbook.keys())
        # no aapl order hanging
        self.assertLessEqual(len(bt.orderbook.orderbook[Asset("AAPL")]), 0)

    def test_swing(self):
        Component().get_handlers().clear()
        bt = PandasBarBacktester(
            lambda a, x: pd.read_csv(f"{path}/../{a.id.lower()}.csv", index_col="Date", parse_dates=True)[x:],
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            '2022-01-01',
            200
        )

        bt.place_order(Order("AAPL", -1, valid_from='2022-01-03'))  #    181
        bt.place_order(Order("AAPL", 2, valid_from='2022-01-28'))   # S+ 169 12
        bt.place_order(Order("AAPL", -2, valid_from='2022-02-23'))  # L- 152 12-17 = -5
        bt.place_order(Order("AAPL", 2, valid_from='2022-03-24'))   # S- 173 -21 +-5 = -26
        bt.place_order(Order("AAPL", -1, valid_from='2022-03-28'))  # L+ 176 -26 + 3 = -23

        dfhist = bt.get_history()
        print(dfhist.columns.tolist())

        np.testing.assert_almost_equal(
            np.array([178.95944214, 174.0597229, 162.27584839, 174.22166443, 178.4495697 ]),
            dfhist.dropna()["AAPL", "quote"].values,
            5
        )

        np.testing.assert_almost_equal(
            np.array([181.877375, 169.45876698, 152.14482246, 173.3840639, 176.186040 ]),
            dfhist.dropna()["AAPL", "trade_price"].values,
            5
        )

        np.testing.assert_almost_equal(
            np.array([0.00000, 12.41861, -4.89534, -26.13458, -23.33260]),
            dfhist.dropna()["TOTAL", "realized_pnl"].values,
            5
        )

        np.testing.assert_almost_equal(
            np.array([-2.91793319, -4.60095592, 10.13102592, -0.8376005, 0.        ]),
            dfhist.dropna()["TOTAL", "unrealized_pnl"].values,
            5
        )

        np.testing.assert_almost_equal(
            np.array([381.87737533, 42.95984137, 347.2494863, 0.48135844, 176.66739847]),
            dfhist.dropna()["$CASH$", "balance"].values,
            5
        )

        np.testing.assert_almost_equal(
            np.array([ -2.91793,   7.81765,   5.23569, -26.97218, -23.3326 ]),
            dfhist.dropna()["TOTAL", "pnl"].values,
            5
        )

        np.testing.assert_almost_equal(
            np.array([-178.95944,  174.05972, -162.27585,  174.22166,    0.     ]),
            dfhist.dropna()["TOTAL", "value"].values,
            5
        )

        np.testing.assert_almost_equal(
            np.array([381.87738,  42.95984, 347.24949,   0.48136, 176.6674]),
            dfhist.dropna()["$CASH$", "balance"].values,
            5
        )

        np.testing.assert_almost_equal(
            (
                np.array([-178.95944, 174.05972, -162.27585, 174.22166, 0.]) +\
                np.array([381.87738, 42.95984, 347.24949, 0.48136, 176.6674])
            ) / 200 - 1,
            dfhist.dropna()["TOTAL", "pnl_%"].values,
            5
        )

        #print(dfhist.dropna())

    def test_target_weights(self):
        Component().get_handlers().clear()
        bt = PandasBarBacktester(
            lambda a, x: pd.read_csv(f"{path}/../{a.id.lower()}.csv", index_col="Date", parse_dates=True)[x:],
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            '2022-01-01',
            100
        )

        for idx in df_aapl.index:
            bt.place_target_weights_oder(TargetWeights({"AAPL": 0.5, "MSFT": 0.5}, valid_from=idx))

        msft_aapl_bah = df_msft["Close"].iloc[-1] / df_msft["Close"].iloc[0] - 1  # -0.26888721433905904
        aapl_bah = df_aapl["Close"].iloc[-1] / df_aapl["Close"].iloc[0] - 1       # -0.16859730629997294
        print(msft_aapl_bah, aapl_bah)

        dfhist = bt.get_history()
        print(len(dfhist), dfhist["TOTAL", "pnl_%"].iloc[-1])
        #print(dfhist)

        self.assertGreater(dfhist["TOTAL", "value"].min(), 70)
        self.assertLess(dfhist["$CASH$", "balance"][1:].max(), 20)

        self.assertGreater(dfhist["TOTAL", "pnl_%"].iloc[-1], msft_aapl_bah)
        self.assertLess(dfhist["TOTAL", "pnl_%"].iloc[-1], aapl_bah)

        # should be approximately -21 %
        self.assertAlmostEqual(dfhist["TOTAL", "pnl_%"].iloc[-1], (msft_aapl_bah + aapl_bah) / 2, 2)

    def test_target_weights_closing(self):
        Component().get_handlers().clear()
        bt = PandasBarBacktester(
            lambda a, x: pd.read_csv(f"{path}/../{a.id.lower()}.csv", index_col="Date", parse_dates=True)[x:],
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            '2022-01-01',
            100
        )

        for i, idx in enumerate(df_aapl.index):
            weights = {"AAPL": 1.0, "MSFT": 0.0} if i % 2 == 0 else {"AAPL": 0.0, "MSFT": 1.0}
            bt.place_target_weights_oder(TargetWeights(weights, valid_from=idx))

        dfhist = bt.get_history()

        # -2 because we need one leading quote to place orders
        # and the last orders (of the last quote) never get filled without a next quote
        self.assertEqual(len(dfhist.dropna()), len(df_aapl) - 2)

        self.assertGreater(dfhist["TOTAL", "value"].min(), 70)
        self.assertLess(dfhist["$CASH$", "balance"][1:].max(), 6)

    @pytest.mark.skipif(False, reason="takes a long time")
    def test_target_weights_performance(self):
        Component().get_handlers().clear()
        bt = PandasBarBacktester(
            lambda a, x: pd.read_csv(f"{path}/../{a.id.lower()[:-3]}.csv", index_col="Date", parse_dates=True)[x:],
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            '2020-01-01',
            starting_balance=100,
            min_target_weight=1e-9,
            order_minimum_quantity=1e-16,
        )

        assets = 300
        target_weights = {f"AAPL{i:03d}": 1.0 / assets for i in range(assets)}
        start = datetime.datetime.now()
        for idx in df_aapl.index:
            bt.place_target_weights_oder(TargetWeights(target_weights, valid_from=idx))

        dfhist = bt.get_history()
        duration = datetime.datetime.now() - start
        print(duration.seconds)

        self.assertGreater(dfhist["TOTAL", "value"].min(), 70)
        self.assertLess(dfhist["$CASH$", "balance"][1:].max(), 5)
        self.assertLessEqual(duration.seconds, 31)
