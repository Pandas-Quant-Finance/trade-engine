from unittest import TestCase

import pandas as pd

from tradeengine.components.component import Component
from tradeengine.components.yfbacktest import YfBacktester
from tradeengine.events import MaximumOrder, CloseOrder

# show all columns
pd.set_option('display.max_columns', None)



class TestYfBacktester(TestCase):

    def test_simple(self):
        Component().get_handlers().clear()
        bt = YfBacktester(
            '2022-01-01',
            100
        )

        bt.place_maximum_order(MaximumOrder("AAPL", valid_from='2022-01-03'))
        bt.place_close_position_order(CloseOrder(None, valid_from='2022-01-28'))

        dfhist = bt.get_history().dropna()
        print(dfhist)

        # 181.8 vs 169.45
        self.assertAlmostEqual(-0.06714, dfhist["AAPL", "pnl_%"].iloc[-1], 5)
        self.assertAlmostEqual(-0.06714, dfhist["TOTAL", "pnl_%"].iloc[-1], 5)
