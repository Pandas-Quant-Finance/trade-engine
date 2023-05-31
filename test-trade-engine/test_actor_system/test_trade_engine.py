import uuid
from pathlib import Path
from time import sleep
from unittest import TestCase

import numpy as np
import pandas as pd
import pykka
from sqlalchemy import create_engine, StaticPool

from testutils.data import ALL_MD_FRAMES, AAPL_MD_FRAMES
from testutils.database import get_sqlite_engine
from testutils.trading import sample_strategy
from tradeengine.actors.memory import PandasQuoteProviderActor, MemPortfolioActor
from tradeengine.actors.sql import SQLOrderbookActor
from tradeengine.actors.sql import SQLPortfolioActor
from tradeengine.backtest import backtest_strategy
from tradeengine.messages import *
from tradeengine.dto.dataflow import Asset


class TestActorTradeEngine(TestCase):

    def test_simple_strategy(self):
        portfolio_actor = SQLPortfolioActor.start(get_sqlite_engine(False))
        orderbook_actor = SQLOrderbookActor.start(portfolio_actor, get_sqlite_engine(False))
        market_actor = PandasQuoteProviderActor.start(portfolio_actor, orderbook_actor, ALL_MD_FRAMES, ["Open", "High", "Low", "Close"])

        try:
            # implement trading system
            signal = sample_strategy()


            # replay market data (blocking)
            market_actor.ask(ReplayAllMarketDataMessage())

        finally:
            # shutdown threadpool
            pykka.ActorRegistry.stop_all()

    def test_long_aapl(self):
        strategy_id: str = str(uuid.uuid4())
        portfolio_actor = MemPortfolioActor.start(funding=100)
        orderbook_actor = SQLOrderbookActor.start(portfolio_actor, get_sqlite_engine(False), strategy_id=strategy_id)
        frames = AAPL_MD_FRAMES.copy()

        strategy = sample_strategy(frames, 'long', slow=30, fast=10, signal_only=False)
        sma_signal_info = {k: v[["ma_fast", "ma_slow"]] for k, v in strategy.items()}
        signal = {k: v["order"] for k, v in strategy.items()}

        backtest = backtest_strategy(
            orderbook_actor,
            portfolio_actor,
            frames,
            signal,
            market_data_extra_data=sma_signal_info
            # shutdown_on_complete=False
        )

        print(backtest.porfolio_performance.columns)
        print(backtest.porfolio_performance.tail())

        file = '../notebooks/strategy-long-aapl.hdf5'
        backtest.save(file)
