from __future__ import annotations
from abc import abstractmethod
from typing import Any, List

import pykka

from tradeengine.messages.messages import NewMarketDataMessage, ReplayAllMarketDataMessage, NewMarketDataProviderMessage

"""
The Quote Provider Actor is responsible to provide market data. This can be by 
 * reading market data from a stream (in a new dedicated thread) and telling his coworkers about 
   new market data
 * by responding to his coworkers whenever they ask for a market data update with the latest available data 
   (for a given timestamp) 
 * by sending market data chronologically for each available asset which is also called "replay"
 
The Actor accepts the following messages:
 * asks about new/recent market data
 * replay a full history of known market data
 
The Actor sends the following messages:
 * tells his coworkers about his existence
 * tells his coworkers about new market data 
"""
class AbstractQuoteProviderActor(pykka.ThreadingActor):

    def __init__(
            self,
            portfolio_actor: pykka.ActorRef,
            orderbook_actor: pykka.ActorRef,
            csv_file_names: List[str],
    ):
        super().__init__()
        self.portfolio_actor = portfolio_actor
        self.orderbook_actor = orderbook_actor
        self.csv_file_names = csv_file_names

        self.portfolio_actor.tell(NewMarketDataProviderMessage(self.actor_ref))

    def on_receive(self, message: Any) -> Any:
        match message:
            case NewMarketDataMessage(asset, as_of):
                return self.fetch_market_data(asset, as_of)
            case ReplayAllMarketDataMessage():
                self.replay_all_market_data()

    @abstractmethod
    def fetch_market_data(self, asset, as_of):
        raise NotImplemented

    def replay_all_market_data(self):
        raise NotImplemented


class CSVQuoteProviderActor(AbstractQuoteProviderActor):

    def fetch_market_data(self, asset, as_of):
        pass

    def replay_all_market_data(self):
        # IMPORTANT always update the portfolio first!
        #self.portfolio_actor.tell()
        #self.orderbook_actor.tell()
        pass
