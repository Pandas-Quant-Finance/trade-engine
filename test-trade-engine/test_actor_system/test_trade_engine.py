import uuid
from pathlib import Path
from unittest import TestCase

from testutils.data import AAPL_MSFT_MD_FRAMES, AAPL_MD_FRAMES, AAPL_MSFT_TLT_MD_FRAMES
from testutils.database import get_sqlite_engine
from testutils.frames import frames_allmost_equal
from testutils.trading import sample_strategy, one_over_n
from tradeengine.actors.memory import MemPortfolioActor
from tradeengine.actors.sql import SQLOrderbookActor
from tradeengine.backtest import backtest_strategy, Backtest

TEST_ROOT = Path(__file__).parents[1]
STRICT = True


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

        file = TEST_ROOT.joinpath('../notebooks/strategy-long-aapl.hdf5')
        backtest.save(file)

        expected_backtest = Backtest.load(Path(__file__).parent.joinpath("strategy-long-aapl.hdf5"))
        frames_allmost_equal(backtest.market_data, expected_backtest.market_data, strict=STRICT)
        frames_allmost_equal(backtest.signals, expected_backtest.signals, strict=STRICT)
        frames_allmost_equal(backtest.orders.drop("strategy_id", axis=1), expected_backtest.orders.drop("strategy_id", axis=1), strict=STRICT)
        frames_allmost_equal(backtest.position_values, expected_backtest.position_values, strict=STRICT)
        frames_allmost_equal(backtest.position_weights, expected_backtest.position_weights, strict=STRICT)
        frames_allmost_equal(backtest.porfolio_performance, expected_backtest.porfolio_performance, strict=STRICT)

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

        file = TEST_ROOT.joinpath('../notebooks/strategy-swing-aapl.hdf5')
        backtest.save(file)

        expected_backtest = Backtest.load(Path(__file__).parent.joinpath("strategy-swing-aapl.hdf5"))
        frames_allmost_equal(backtest.market_data, expected_backtest.market_data, strict=STRICT)
        frames_allmost_equal(backtest.signals, expected_backtest.signals, strict=STRICT)
        frames_allmost_equal(backtest.orders.drop("strategy_id", axis=1), expected_backtest.orders.drop("strategy_id", axis=1), strict=STRICT)
        frames_allmost_equal(backtest.position_values, expected_backtest.position_values, strict=STRICT)
        frames_allmost_equal(backtest.position_weights, expected_backtest.position_weights, strict=STRICT)
        frames_allmost_equal(backtest.porfolio_performance, expected_backtest.porfolio_performance, strict=STRICT)

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

        file = TEST_ROOT.joinpath('../notebooks/strategy-swing-all.hdf5')
        backtest.save(file)

        expected_backtest = Backtest.load(Path(__file__).parent.joinpath("strategy-swing-all.hdf5"))
        frames_allmost_equal(backtest.market_data, expected_backtest.market_data, strict=STRICT)
        frames_allmost_equal(backtest.signals, expected_backtest.signals, strict=STRICT)
        frames_allmost_equal(backtest.orders.drop("strategy_id", axis=1), expected_backtest.orders.drop("strategy_id", axis=1), strict=STRICT)
        frames_allmost_equal(backtest.position_values, expected_backtest.position_values, strict=STRICT)
        frames_allmost_equal(backtest.position_weights, expected_backtest.position_weights, strict=STRICT)
        frames_allmost_equal(backtest.porfolio_performance, expected_backtest.porfolio_performance, strict=STRICT)

    def test_long_1oN(self):
        strategy_id: str = str(uuid.uuid4())
        portfolio_actor = MemPortfolioActor.start(funding=100_000)
        orderbook_actor = SQLOrderbookActor.start(portfolio_actor, get_sqlite_engine(False), strategy_id=strategy_id)
        frames = AAPL_MSFT_TLT_MD_FRAMES.copy()

        signal = one_over_n(frames)

        backtest = backtest_strategy(
            orderbook_actor,
            portfolio_actor,
            frames,
            signal,
        )

        file = TEST_ROOT.joinpath('../notebooks/strategy-long-1oN.hdf5')
        backtest.save(file)

        expected_backtest = Backtest.load(Path(__file__).parent.joinpath("strategy-long-1oN.hdf5"))
        frames_allmost_equal(backtest.market_data, expected_backtest.market_data, strict=STRICT)
        frames_allmost_equal(backtest.signals, expected_backtest.signals, strict=STRICT)
        frames_allmost_equal(backtest.orders.drop("strategy_id", axis=1), expected_backtest.orders.drop("strategy_id", axis=1), strict=STRICT)
        frames_allmost_equal(backtest.position_values, expected_backtest.position_values, strict=STRICT)
        frames_allmost_equal(backtest.position_weights, expected_backtest.position_weights, strict=STRICT)
        frames_allmost_equal(backtest.porfolio_performance, expected_backtest.porfolio_performance, strict=STRICT)

