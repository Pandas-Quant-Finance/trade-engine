from __future__ import annotations

import logging
from typing import List, Dict, Tuple

import pandas as pd
import pykka

from tradeengine.actors.market_data_actor import AbstractQuoteProviderActor
from tradeengine.dto import Asset
from tradeengine.messages.messages import NewBidAskMarketData, NewBarMarketData
from tqdm import tqdm

LOG = logging.getLogger(__name__)


class PandasQuoteProviderActor(AbstractQuoteProviderActor):

    def __init__(
            self,
            portfolio_actor: pykka.ActorRef,
            orderbook_actor: pykka.ActorRef,
            dataframes: Dict[Asset, pd.DataFrame],
            columns: List,
            portfolio_update_timeout: int = 60,
            blocking: bool = True,
    ):
        super().__init__(portfolio_actor, orderbook_actor, portfolio_update_timeout)
        self.dataframe: pd.DataFrame = pd.concat([df[columns] for df in dataframes.values()], axis=1, keys=dataframes.keys()).sort_index().ffill()
        self.assets = list(dataframes.keys())
        self.columns = columns
        self.blocking = blocking

        self.is_bar = len(columns) == 4

    def on_stop(self) -> None:
        LOG.debug(f"stopped market data actor {self}")

    def replay_all_market_data(self) -> pd.DataFrame:
        # IMPORTANT always update the portfolio first!
        for tst, row in tqdm(self.dataframe.iterrows(), total=len(self.dataframe)):
            tst = tst.to_pydatetime() if isinstance(tst, pd.Timestamp) else tst

            for asset in self.assets:
                price_data = row[asset]
                message = NewBarMarketData(
                    asset, tst, *price_data.values
                ) if self.is_bar else NewBidAskMarketData(
                    asset, tst, price_data[self.columns[0]], price_data[self.columns[1 if len(self.columns) > 1 else 0]],
                )

                # use ask to be sure portfolio has all data processed before we execute orders
                self.portfolio_actor.ask(message)
                self.orderbook_actor.ask(message, block=self.blocking)

        df = self.dataframe.rename(columns=str, level=0)
        return df