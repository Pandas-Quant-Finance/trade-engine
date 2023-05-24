import logging
from contextlib import closing
from unittest import TestCase, skip

import pandas as pd
import yfinance

from tradeengine._obsolete.components.yfbacktest import PandasBarBacktester
from tradeengine._obsolete.events import *




class YfBackTest(TestCase):

    @skip("only needed to create test data")
    def test_dwonload_tickers(self):
        tickers = ['ATVI', 'ADBE', 'ADP', 'ABNB', 'ALGN', 'GOOGL', 'GOOG', 'AMZN', 'AMD', 'AEP', 'AMGN', 'ADI', 'ANSS', 'AAPL',
         'AMAT', 'ASML', 'AZN', 'TEAM', 'ADSK', 'BKR', 'BIIB', 'BKNG', 'AVGO', 'CDNS', 'CHTR', 'CTAS', 'CSCO', 'CTSH',
         'CMCSA', 'CEG', 'CPRT', 'CSGP', 'COST', 'CRWD', 'CSX', 'DDOG', 'DXCM', 'FANG', 'DLTR', 'EBAY', 'EA', 'ENPH',
         'EXC', 'FAST', 'FISV', 'FTNT', 'GILD', 'GFS', 'HON', 'IDXX', 'ILMN', 'INTC', 'INTU', 'ISRG', 'JD', 'KDP',
         'KLAC', 'KHC', 'LRCX', 'LCID', 'LULU', 'MAR', 'MRVL', 'MELI', 'META', 'MCHP', 'MU', 'MSFT', 'MRNA', 'MDLZ',
         'MNST', 'NFLX', 'NVDA', 'NXPI', 'ORLY', 'ODFL', 'PCAR', 'PANW', 'PAYX', 'PYPL', 'PEP', 'PDD', 'QCOM', 'REGN',
         'RIVN', 'ROST', 'SGEN', 'SIRI', 'SBUX', 'SNPS', 'TMUS', 'TSLA', 'TXN', 'VRSK', 'VRTX', 'WBA', 'WBD', 'WDAY',
         'XEL', 'ZM', 'ZS']

        for ticker in tickers:
            print(f"download {ticker}")
            yfinance.Ticker(ticker).history('max').to_hdf('../quotes.hd5', key=ticker)

    def _test_test(self):
        print()
        logging.basicConfig(level=logging.DEBUG)

        bt = PandasBarBacktester(
            lambda asset, time: pd.read_hdf('../quotes.hd5', key=asset.id)[time:],
            '2020-01-01',
            # starting_balance = 100
        )

        #bt = YfBacktester('2020-01-01')
        bt.trade(Order(Asset("AAPL"), 10, valid_from='2020-01-13'))

        bt.trade(Order(Asset("AAPL"), -10, valid_from='2020-01-20'))
        bt.trade(Order(Asset("MSFT"), -10, valid_from='2020-01-23'))

        print(bt.position_value_balance, bt.cash_balance)
        hist = bt.get_history()
        print(hist)

        self.assertEqual(
            str(hist["AAPL"].dropna().index.tolist()),
            "[Timestamp('2020-01-14 00:00:00-0500', tz='America/New_York'), Timestamp('2020-01-15 00:00:00-0500', tz='America/New_York'), Timestamp('2020-01-16 00:00:00-0500', tz='America/New_York'), Timestamp('2020-01-17 00:00:00-0500', tz='America/New_York'), Timestamp('2020-01-21 00:00:00-0500', tz='America/New_York')]"
        )
        self.assertListEqual(
            hist["AAPL"].dropna()["quantity"].tolist(),
            [10.0, 10.0, 10.0, 10.0, 0.0]
        )

        self.assertAlmostEqual(bt.cash_balance, 1729.702777, 5)
        self.assertAlmostEqual(bt.position_value_balance, -2415.800018, 5)

    def _test_1overNof100(self):
        print()
        logging.basicConfig(level=logging.DEBUG)

        filename = '../quotes.hd5'
        with closing(pd.HDFStore(filename)) as store:
            tickers = [k[1:] for k in store.keys()]

        bt = PandasBarBacktester(
            lambda asset, time: pd.read_hdf(filename, key=asset.id)[time:],
            '2020-01-19',
            # starting_balance = 100
        )

        # make 1/n portfolio for all tickers
        bt.target_weights(TargetWeights({Asset(t): 1.0 / len(tickers) for t in tickers}, valid_from='2022-01-19'))
        hist = bt.get_history()
        print(bt.position_value_balance, bt.cash_balance, sum(bt.get_current_weights('2022-12-31').values()), "\n", bt.get_current_weights('2022-12-31'))
