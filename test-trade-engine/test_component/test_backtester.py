from unittest import TestCase

import numpy as np
import pandas as pd
import os
from tradeengine.components.backtester import PandasBarBacktester
from tradeengine.events import Bar, MaximumOrder, CloseOrder, Order, TargetWeights

# show all columns
pd.set_option('display.max_columns', None)

path = os.path.dirname(os.path.abspath(__file__))
df_aapl = pd.read_csv(f"{path}/../aapl.csv", index_col="Date", parse_dates=True)['2022-01-01':]
df_msft = pd.read_csv(f"{path}/../msft.csv", index_col="Date", parse_dates=True)['2022-01-01':]


class TestBackTester(TestCase):

    def test_simple(self):
        bt = PandasBarBacktester(
            lambda a, x: pd.read_csv(f"{path}/../{a.id.lower()}.csv", index_col="Date", parse_dates=True)[x:],
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            '2022-01-01',
            100
        )

        bt.place_maximum_order(MaximumOrder("AAPL", valid_from='2022-01-03'))
        bt.close_position(CloseOrder(None, valid_from='2022-01-28'))

        # the pnl should be very close to just the move of the stock
        buy_and_hold = ((df_aapl.loc[:"2022-02-01", "Close"].pct_change().fillna(0) + 1).cumprod() - 1)
        #print(df.iloc[0, 3], df.iloc[-1, 3])

        dfhist = bt.get_history()
        print(dfhist.dropna())

        # 181.8 vs 169.45
        self.assertAlmostEqual(-0.06714, dfhist["AAPL", "pnl_%"].iloc[-1], 5)
        self.assertAlmostEqual(-0.06714, dfhist["TOTAL", "pnl_%"].iloc[-1], 5)
        self.assertLess(dfhist["TOTAL", "pnl_%"].iloc[-1], buy_and_hold.iloc[-1])
        self.assertLess(dfhist["AAPL", "pnl_%"].iloc[-1], buy_and_hold.iloc[-1])

    def test_swing(self):
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
            np.array([-0.01458967, 0.03908826, 0.02617845, -0.13486089, -0.11666301]),
            dfhist.dropna()["TOTAL", "pnl_%"].values,
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

        #print(dfhist.dropna())


    def test_target_weights(self):
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
        print(dfhist["TOTAL", "pnl_%"].iloc[-1])
        #print(dfhist)

        self.assertGreater(dfhist["TOTAL", "pnl_%"].iloc[-1], msft_aapl_bah)
        self.assertLess(dfhist["TOTAL", "pnl_%"].iloc[-1], aapl_bah)
        self.assertAlmostEqual(dfhist["TOTAL", "pnl_%"].iloc[-1], (msft_aapl_bah + aapl_bah) / 2, 2)
