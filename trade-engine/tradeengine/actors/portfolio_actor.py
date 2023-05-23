from __future__ import annotations

from datetime import datetime

import pandas as pd
from sqlalchemy import Engine
from abc import abstractmethod
from typing import Any, Tuple

import pykka

from tradeengine.dto.dataflow import PositionValue, PortfolioValue
from tradeengine.messages.messages import PortfolioValueMessage, QuoteAskMarketData, \
    NewBidAskMarketData, NewBarMarketData, NewPositionMessage, PortfolioPerformanceMessage, PortfolioTradesMessage

"""
The Portfolio Actor is responsible to keep track of the entire Portfolio of a Client/Strategy. 
It keeps track of the current positions and evaluations as well as it builds up a complete history
of positions entering and leaving the portfolio.

The Portfolio Action accepts the following messages:
 * a message which tells the portfolio about a market data actor (ref) which can be asked about quote updates
 * a message which tells the portfolio about new market quote updates to re-evaluate its position value
 * a message of actor asking about the current portfolio value
 * a message of actor asking about a current position value
 
The actor sends the following messages:
 * asks the Quote provider Actor about a current quote    
"""
class AbstractPortfolioActor(pykka.ThreadingActor):

    def __init__(
            self,
            funding: float = 1.0,
    ):
        super().__init__()
        self.funding = funding
        # self.quote_provider: pykka.ActorRef | None = None

    def on_receive(self, message: Any) -> Any:
        match message:
            #case NewMarketDataProviderMessage(provider):
            #     self.quote_provider = provider

            case PortfolioValueMessage(as_of):
                return self.get_portfolio_value(as_of)
            case PortfolioTradesMessage(as_of):
                return self.get_trades(as_of)
            case PortfolioPerformanceMessage(as_of, resample_rule):
                return self.get_performance_history(as_of, resample_rule)

            case NewPositionMessage(asset, as_of, quantity, price, fee):
                return self.add_new_position(asset, as_of, quantity, price, fee)
            case QuoteAskMarketData(asset, as_of, quote):
                return self.update_position_value(asset, as_of, quote, quote)
            case NewBidAskMarketData(asset, as_of, bid, ask):
                return self.update_position_value(asset, as_of, bid, ask)
            case NewBarMarketData(asset, as_of, open, high, low, close):
                return self.update_position_value(asset, as_of, close, close)
            case _:
                raise ValueError(f"Unknown Message {message}")

    def get_performance_history(self, as_of: datetime = None, resample_rule=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if as_of is None: as_of = datetime.max

        df = self.get_portfolio_timeseries(as_of).pivot(index='time', columns='asset', values='value').ffill()

        if resample_rule is not None:
            df.resample(resample_rule, convention='e').last()

        df2 = pd.DataFrame({}, index=df.index)
        df2['value'] = df.fillna(0).sum(axis=1)
        df2['return'] = df2['value'].pct_change().fillna(0)
        df2['performance'] = (df2['return'] + 1).cumprod()

        return df, df2

    @abstractmethod
    def get_portfolio_timeseries(self, as_of: datetime | None = None) -> pd.DataFrame:
        raise NotImplemented

    @abstractmethod
    def get_trades(self, as_of: datetime | None = None) -> pd.DataFrame:
        raise NotImplemented

    @abstractmethod
    def get_portfolio_value(self, as_of: datetime | None = None) -> PortfolioValue:
        raise NotImplemented

    @abstractmethod
    def add_new_position(self, asset, as_of, quantity, price, fee):
        pass

    @abstractmethod
    def update_position_value(self, asset, as_of, bid, ask):
        raise NotImplemented

