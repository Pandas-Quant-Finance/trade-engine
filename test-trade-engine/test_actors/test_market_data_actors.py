from pathlib import Path
from unittest import TestCase

import pandas as pd

from tradeengine.actors.market_data_actor import PandasQuoteProviderActor
from tradeengine.dto.dataflow import Asset


class MockActor():
    def ask(self, *args): print(*args)
    def tell(self, *args): print(*args)


class TestMarketDataActors(TestCase):

    def test_pandas_market_data(self):
        frames = {
            Asset(ticker): pd.read_csv(Path(__file__).parents[1].joinpath(f"{ticker.lower()}.csv"), parse_dates=True, index_col="Date")
            for ticker in ["AAPL", "MSFT"]
        }

        actor = PandasQuoteProviderActor(MockActor(), MockActor, frames, ["Open", "High", "Low", "Close"])
        actor.replay_all_market_data()