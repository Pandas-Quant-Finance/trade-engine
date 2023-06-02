import uuid
from pathlib import Path
from time import sleep
from unittest import TestCase

import numpy as np
import pandas as pd
import pykka
from sqlalchemy import create_engine, StaticPool

from testutils.data import AAPL_MSFT_MD_FRAMES, AAPL_MD_FRAMES
from testutils.database import get_sqlite_engine
from testutils.trading import sample_strategy
from tradeengine.actors.memory import PandasQuoteProviderActor, MemPortfolioActor
from tradeengine.actors.sql import SQLOrderbookActor
from tradeengine.actors.sql import SQLPortfolioActor
from tradeengine.backtest import backtest_strategy, Backtest
from tradeengine.messages import *
from tradeengine.dto.dataflow import Asset


class TestActorTradeEngine(TestCase):

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

        file = '../notebooks/strategy-long-aapl.hdf5'
        backtest.save(file)

        expected_backtest = Backtest.load(Path(__file__).parent.joinpath("strategy-long-aapl.hdf5"))
        pd.testing.assert_frame_equal(backtest.market_data, expected_backtest.market_data)
        pd.testing.assert_frame_equal(backtest.signals, expected_backtest.signals)
        pd.testing.assert_frame_equal(backtest.orders.drop("strategy_id", axis=1), expected_backtest.orders.drop("strategy_id", axis=1))
        pd.testing.assert_frame_equal(backtest.position_values, expected_backtest.position_values)
        pd.testing.assert_frame_equal(backtest.position_weights, expected_backtest.position_weights)
        pd.testing.assert_frame_equal(backtest.porfolio_performance, expected_backtest.porfolio_performance)

    def test_swing_aapl(self):
        strategy_id: str = str(uuid.uuid4())
        portfolio_actor = MemPortfolioActor.start(funding=100)
        orderbook_actor = SQLOrderbookActor.start(portfolio_actor, get_sqlite_engine(False), strategy_id=strategy_id)
        frames = AAPL_MD_FRAMES.copy()

        strategy = sample_strategy(frames, 'swing', slow=30, fast=10, signal_only=False)
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

        file = '../notebooks/strategy-swing-aapl.hdf5'
        backtest.save(file)

        expected_backtest = Backtest.load(Path(__file__).parent.joinpath("strategy-swing-aapl.hdf5"))
        pd.testing.assert_frame_equal(backtest.market_data, expected_backtest.market_data)
        pd.testing.assert_frame_equal(backtest.signals, expected_backtest.signals)
        pd.testing.assert_frame_equal(backtest.orders.drop("strategy_id", axis=1), expected_backtest.orders.drop("strategy_id", axis=1))
        pd.testing.assert_frame_equal(backtest.position_values, expected_backtest.position_values)
        pd.testing.assert_frame_equal(backtest.position_weights, expected_backtest.position_weights)
        pd.testing.assert_frame_equal(backtest.porfolio_performance, expected_backtest.porfolio_performance)

    def test_swing_all(self):
        strategy_id: str = str(uuid.uuid4())
        portfolio_actor = MemPortfolioActor.start(funding=100)
        orderbook_actor = SQLOrderbookActor.start(portfolio_actor, get_sqlite_engine(False), strategy_id=strategy_id)
        frames = AAPL_MSFT_MD_FRAMES.copy()

        strategy = sample_strategy(AAPL_MSFT_MD_FRAMES, 'swing', slow=30, fast=10, signal_only=False)
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

        file = '../notebooks/strategy-swing-all.hdf5'
        backtest.save(file)

        expected_backtest = Backtest.load(Path(__file__).parent.joinpath("strategy-swing-all.hdf5"))
        pd.testing.assert_frame_equal(backtest.market_data, expected_backtest.market_data)
        pd.testing.assert_frame_equal(backtest.signals, expected_backtest.signals)
        pd.testing.assert_frame_equal(backtest.orders.drop("strategy_id", axis=1), expected_backtest.orders.drop("strategy_id", axis=1))
        pd.testing.assert_frame_equal(backtest.position_values, expected_backtest.position_values)
        pd.testing.assert_frame_equal(backtest.position_weights, expected_backtest.position_weights)
        pd.testing.assert_frame_equal(backtest.porfolio_performance, expected_backtest.porfolio_performance)

    def test_long_1oN(self):
        strategy_id: str = str(uuid.uuid4())
        portfolio_actor = MemPortfolioActor.start(funding=100)
        orderbook_actor = SQLOrderbookActor.start(portfolio_actor, get_sqlite_engine(False), strategy_id=strategy_id)
        frames = AAPL_MSFT_MD_FRAMES.copy()

        # FIXME use one_over_n
        strategy = sample_strategy(AAPL_MSFT_MD_FRAMES, 'swing', slow=30, fast=10, signal_only=False)
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

        file = '../notebooks/strategy-swing-all.hdf5'
        backtest.save(file)

        expected_backtest = Backtest.load(Path(__file__).parent.joinpath("strategy-swing-all.hdf5"))
        pd.testing.assert_frame_equal(backtest.market_data, expected_backtest.market_data)
        pd.testing.assert_frame_equal(backtest.signals, expected_backtest.signals)
        pd.testing.assert_frame_equal(backtest.orders.drop("strategy_id", axis=1), expected_backtest.orders.drop("strategy_id", axis=1))
        pd.testing.assert_frame_equal(backtest.position_values, expected_backtest.position_values)
        pd.testing.assert_frame_equal(backtest.position_weights, expected_backtest.position_weights)
        pd.testing.assert_frame_equal(backtest.porfolio_performance, expected_backtest.porfolio_performance)

