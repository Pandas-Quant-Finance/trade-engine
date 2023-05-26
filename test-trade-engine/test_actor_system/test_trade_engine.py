import uuid
from pathlib import Path
from time import sleep
from unittest import TestCase

import numpy as np
import pandas as pd
import pykka
from sqlalchemy import create_engine, StaticPool

from testutils.data import FRAMES
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
        market_actor = PandasQuoteProviderActor.start(portfolio_actor, orderbook_actor, FRAMES, ["Open", "High", "Low", "Close"])

        try:
            # implement trading system
            signal = sample_strategy()


            # replay market data (blocking)
            market_actor.ask(ReplayAllMarketDataMessage())

        finally:
            # shutdown threadpool
            pykka.ActorRegistry.stop_all()

    def test_foo(self):
        strategy_id: str = str(uuid.uuid4())
        portfolio_actor = MemPortfolioActor.start(funding=100)
        orderbook_actor = SQLOrderbookActor.start(portfolio_actor, get_sqlite_engine(False), strategy_id=strategy_id)

        md, orders, val, weight, performance = backtest_strategy(
            orderbook_actor,
            portfolio_actor,
            FRAMES,
            sample_strategy('long', slow=30, fast=10),
            # shutdown_on_complete=False
        )

        print(performance.columns)
        print(performance.tail())

        file = '../notebooks/strategy.hdf5'
        md.to_hdf(file, key='MD')
        orders.to_hdf(file, key='ORDERS')
        val.to_hdf(file, key='POS_VAL')
        weight.to_hdf(file, key='POS_WGT')
        performance.to_hdf(file, key='PORTFOLIO')
