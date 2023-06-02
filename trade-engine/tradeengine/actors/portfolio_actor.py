from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime
from typing import Any, Tuple

import numpy as np
import pandas as pd
import pykka

from tradeengine.dto.dataflow import PortfolioValue
from tradeengine.messages.messages import PortfolioValueMessage, \
    NewBidAskMarketData, NewBarMarketData, NewPositionMessage, PortfolioPerformanceMessage

LOG = logging.getLogger(__name__)


class AbstractPortfolioActor(pykka.ThreadingActor):
    """
    The Portfolio Actor is responsible to keep track of the entire Portfolio of a Client/Strategy.
    It keeps track of the current positions and evaluations as well as it builds up a complete history
    of positions entering and leaving the portfolio.

    The Portfolio Action accepts the following messages:
     * a message which tells the portfolio about new market quote updates to re-evaluate its position value
     * a message of actor asking about the current portfolio value
     * messages about portfolio statistics

    The actor sends the following messages:
     *

    """

    def __init__(
            self,
            funding: float = 1.0,
    ):
        super().__init__()
        self.funding = funding
        # self.quote_provider: pykka.ActorRef | None = None

    def on_stop(self) -> None:
        LOG.debug(f"stopped orderbook actor {self}")

    def on_receive(self, message: Any) -> Any:
        match message:
            #case NewMarketDataProviderMessage(provider):
            #     self.quote_provider = provider

            case PortfolioValueMessage(as_of):
                return self.get_portfolio_value(as_of)
            case PortfolioPerformanceMessage(as_of, resample_rule):
                return self.get_performance_history(as_of, resample_rule)

            case NewPositionMessage(asset, as_of, quantity, price, fee):
                return self.add_new_position(asset, as_of, quantity, price, fee)
            case NewBidAskMarketData(asset, as_of, bid, ask):
                return self.update_position_value(asset, as_of, bid, ask)
            case NewBarMarketData(asset, as_of, open, high, low, close):
                return self.update_position_value(asset, as_of, close, close)
            case _:
                raise ValueError(f"Unknown Message {message}")

    def get_performance_history(self, as_of: datetime = None, resample_rule=None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if as_of is None: as_of = datetime.max

        df = self.get_portfolio_timeseries(as_of)
        df_pos_val = df.pivot_table(index='time', columns='asset', values='value', aggfunc='last').sort_index().ffill()

        if resample_rule is not None:
            df_pos_val.resample(resample_rule, convention='e').last()

        df_pos_weight = df_pos_val / np.sum(df_pos_val.values, axis=1, keepdims=True)

        df_portfolio = pd.DataFrame({}, index=df_pos_val.index)
        df_portfolio['value'] = df_pos_val.fillna(0).sum(axis=1)
        df_portfolio['return'] = df_portfolio['value'].pct_change().fillna(0)
        df_portfolio['performance'] = (df_portfolio['return'] + 1).cumprod()

        return df_pos_val, df_pos_weight, df_portfolio

    @abstractmethod
    def get_portfolio_timeseries(self, as_of: datetime | None = None) -> pd.DataFrame:
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

