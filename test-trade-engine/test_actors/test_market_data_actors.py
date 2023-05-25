from pathlib import Path
from unittest import TestCase

import pandas as pd
import pandas.testing

from test_utils.mocks import MockActor
from tradeengine.actors.memory.market_data_actor import PandasQuoteProviderActor
from tradeengine.dto.dataflow import Asset




class TestMarketDataActors(TestCase):

    def test_pandas_market_data(self):
        frames = {
            Asset(ticker): pd.read_csv(Path(__file__).parents[1].joinpath(f"{ticker.lower()}.csv"), parse_dates=True, index_col="Date")
            for ticker in ["AAPL", "MSFT"]
        }
        pa = MockActor()
        oba = MockActor()
        actor = PandasQuoteProviderActor(pa, oba, frames, ["Open", "High", "Low", "Close"])
        actor.replay_all_market_data()

        df = pd.DataFrame(pa.received)
        self.assertListEqual(df["as_of"].to_list(), df["as_of"].sort_values().to_list())

