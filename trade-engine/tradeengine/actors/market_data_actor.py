from __future__ import annotations

import logging
from typing import Any

import pykka

from tradeengine.messages.messages import ReplayAllMarketDataMessage, \
    NewBidAskMarketData, NewBarMarketData

LOG = logging.getLogger(__name__)


class AbstractQuoteProviderActor(pykka.ThreadingActor):
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

    def on_stop(self) -> None:
        LOG.debug(f"stopped orderbook actor {self}")

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

