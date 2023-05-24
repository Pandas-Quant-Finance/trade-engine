from __future__ import annotations

from typing import Any, List, Dict

import pandas as pd
import pykka

from tradeengine.dto.dataflow import Asset
from tradeengine.messages.messages import ReplayAllMarketDataMessage, \
    NewBidAskMarketData, NewBarMarketData

"""
The Quote Provider Actor is responsible to provide market data. This can be by 
 * reading market data from a stream (in a new dedicated thread) and telling his coworkers about 
   new market data
 * by responding to his coworkers whenever they ask for a market data update with the latest available data 
   (for a given timestamp) 
 * by sending market data chronologically for each available asset which is also called "replay"
 
The Actor accepts the following messages:
 * replay a full history of known market data
 
The Actor accepts (as of a proxy) and sends the following messages:
 * tells his coworkers about his existence
 * tells his coworkers about new market data 
"""
class AbstractQuoteProviderActor(pykka.ThreadingActor):

    def __init__(
            self,
            portfolio_actor: pykka.ActorRef,
            orderbook_actor: pykka.ActorRef,
            portfolio_update_timeout: int = 60,
    ):
        super().__init__()
        self.portfolio_actor = portfolio_actor
        self.orderbook_actor = orderbook_actor
        self.portfolio_update_timeout = portfolio_update_timeout

    def on_receive(self, message: Any) -> Any:
        match message:
            case NewBidAskMarketData() | NewBarMarketData():
                # make sure the portfolio has processed everything (use ask) before executing orders
                self.portfolio_actor.ask(message, timeout=self.portfolio_update_timeout)
                self.orderbook_actor.tell(message)
                return

            case ReplayAllMarketDataMessage():
                self.replay_all_market_data()

    def replay_all_market_data(self):
        raise NotImplemented


class PandasQuoteProviderActor(AbstractQuoteProviderActor):

    def __init__(
            self,
            portfolio_actor: pykka.ActorRef,
            orderbook_actor: pykka.ActorRef,
            dataframes: Dict[Asset, pd.DataFrame],
            columns: List,
            portfolio_update_timeout: int = 60,
    ):
        super().__init__(portfolio_actor, orderbook_actor, portfolio_update_timeout)
        self.dataframe: pd.DataFrame = pd.concat([df[columns] for df in dataframes.values()], axis=1, keys=dataframes.keys()).sort_index().ffill()
        self.assets = list(dataframes.keys())
        self.columns = columns
        self.is_bar = len(columns) == 4

    def replay_all_market_data(self):
        # IMPORTANT always update the portfolio first!
        #self.portfolio_actor.ask()  # ask to be sure portfolio has all data
        #self.orderbook_actor.tell()
        for tst, row in self.dataframe.iterrows():
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
                self.orderbook_actor.tell(message)

