from pathlib import Path
from time import sleep
from unittest import TestCase

import numpy as np
import pandas as pd
import pykka
from sqlalchemy import create_engine, StaticPool

from test_utils.data import FRAMES
from test_utils.database import get_sqlite_engine
from test_utils.trading import sample_strategy
from tradeengine.actors.memory import PandasQuoteProviderActor
from tradeengine.actors.sql import SQLOrderbookActor
from tradeengine.actors.sql import SQLPortfolioActor
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

