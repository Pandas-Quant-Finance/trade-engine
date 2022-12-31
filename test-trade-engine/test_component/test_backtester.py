from unittest import TestCase

import pandas as pd
import os
from tradeengine.components.backtester import PandasBarBacktester
from tradeengine.events import Bar, MaximumOrder, CloseOrder

path = os.path.dirname(os.path.abspath(__file__))

class TestBackTester(TestCase):

    def test_simple(self):
        df = pd.read_csv(f"{path}/../aapl.csv", index_col="Date", parse_dates=True)['2022-01-01':]

        bt = PandasBarBacktester(
            lambda a, x: pd.read_csv(f"{path}/../{a.id.lower()}.csv", index_col="Date", parse_dates=True)[x:],
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            '2022-01-01',
            100
        )

        bt.place_maximum_order(MaximumOrder("AAPL", valid_from='2022-01-03'))
        bt.close_position(CloseOrder(None, valid_from='2022-01-28'))

        # the pnl should be very close to just the move of the stock
        buy_and_hold = ((df.loc[:"2022-02-01", "Close"].pct_change().fillna(0) + 1).cumprod() - 1)
        #print(df.iloc[0, 3], df.iloc[-1, 3])

        dfhist = bt.get_history()
        #print(dfhist)

        self.assertAlmostEqual(-0.06714, dfhist["TOTAL", "pnl_%"].iloc[-1], 5)
        self.assertLess(dfhist["TOTAL", "pnl_%"].iloc[-1], buy_and_hold.iloc[-1])

    def test_swing(self):
        pass
