from unittest import TestCase

import numpy as np
import yfinance as yf

#from tradeengine import YFinanceBacktestingTradeEngine
from tradeengine._obsolete.components import YfBacktester
from tradeengine._obsolete.events import Order, Asset


class TestExampleStrategy(TestCase):

    def test_200_ma(self):
        df = yf.Ticker("^GSPC").history(start='2000-01-01')
        df["SP-Returns"] = df["Close"].pct_change()
        df["200-MA"] = df["Close"].rolling(200).mean()
        df = df.dropna()

        df["Position"] = np.where(df["Close"] > df["200-MA"], 1, 0)
        df["200-MA-Returns"] = df["Position"].shift(1) * df["SP-Returns"]
        df["bh"] = (df["SP-Returns"] + 1).cumprod() - 1
        df["200"] = (df["200-MA-Returns"] + 1).cumprod() - 1
        print("\n", df["bh"].iloc[-1], df["200"].iloc[-1])

        # now simulate the same strategy with the trade engine where no shift is needed
        # since we inherently only allow to trade at the next bar _after_ the signal
        te = YfBacktester(df.index[0])
        has_position = False
        for idx, day in df[["Close", "200-MA"]].iterrows():
            if day["Close"] > day["200-MA"] and not has_position:
                te.trade(Order(Asset("^GSPC"), 1, valid_from=idx, position_id='SP500'))
                has_position = True
            elif day["Close"] < day["200-MA"] and has_position:
                te.trade(Order(Asset("^GSPC"), -1, valid_from=idx, position_id='SP500'))
                has_position = False

        print(te.get_history().iloc[-1])

        self.assertLess(df["200"].iloc[-1], df["bh"].iloc[-1])
        self.assertLess(te.get_history()["TOTAL", "pnl_percent"].iloc[-1], df["200"].iloc[-1],)

    def test_200_ma_compounding(self):
        df = yf.Ticker("^GSPC").history(start='2000-01-01')
        df["SP-Returns"] = df["Close"].pct_change()
        df["200-MA"] = df["Close"].rolling(200).mean()
        df = df.dropna()

        df["Position"] = np.where(df["Close"] > df["200-MA"], 1, 0)
        df["200-MA-Returns"] = df["Position"].shift(1) * df["SP-Returns"]
        df["bh"] = (df["SP-Returns"] + 1).cumprod() - 1
        df["200"] = (df["200-MA-Returns"] + 1).cumprod() - 1
        print("\n", df["bh"].iloc[-1], df["200"].iloc[-1])

        # now simulate the same strategy with the trade engine where no shift is needed
        # since we inherently only allow to trade at the next bar _after_ the signal
        te = YFinanceBacktestingTradeEngine(start_capital=100)
        for idx, day in df[["Close", "200-MA"]].iterrows():
            if day["Close"] > day["200-MA"]:
                te.trade("^GSPC", 'max', slippage=0.0, timestamp=idx)
            elif day["Close"] < day["200-MA"]:
                te.close("^GSPC", slippage=0.0, timestamp=idx)

        print(te.get_history()["TOTAL", "pnl_percent"].iloc[-1])

        self.assertLess(df["200"].iloc[-1], df["bh"].iloc[-1])
        self.assertLess(te.get_history()["TOTAL", "pnl_percent"].iloc[-1], df["bh"].iloc[-1])

