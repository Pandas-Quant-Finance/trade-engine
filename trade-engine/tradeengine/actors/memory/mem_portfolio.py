from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

from tradeengine.actors.portfolio_actor import AbstractPortfolioActor
from tradeengine.actors.sql.persitency import PortfolioPosition
from tradeengine.dto.dataflow import PositionValue, PortfolioValue, Asset, CASH

LOG = logging.getLogger(__name__)
FUNDING_DATE = datetime.utcnow().replace(year=1900, month=1, day=1)


class MemPortfolioActor(AbstractPortfolioActor):

    def __init__(
            self,
            funding: float = 1.0,
            strategy_id: str = '',
            funding_date: datetime = FUNDING_DATE
    ):
        super().__init__(funding)
        self.strategy_id = strategy_id
        self.positions: Dict[Asset, PortfolioPosition] = {}
        self.funding_date = funding_date

        self.portfolio_history: List[pd.Series] = []

        # in case we have an empty portfolio initialize the cash position
        if len(self.positions) <= 0:
            self.positions[CASH] = PortfolioPosition(strategy_id=self.strategy_id, asset=CASH, time=funding_date, quantity=funding, cost_basis=1.0, value=funding)
            self.update_position_value(CASH, funding_date, 1.0, 1.0)

    def add_new_position(self, asset, as_of, quantity, price, fee):
        assert as_of > self.funding_date, f"can't add trades before the portfolio was funded! {as_of} > {self.funding_date}"
        assert as_of >= self.positions.get(asset, PortfolioPosition(time=as_of)).time, \
            f"Can't backdate positions! {self.positions.get(asset, PortfolioPosition(time=as_of)).time} > {as_of}"

        # every trade as a cost aspect as in cash
        cost = -quantity * price - fee

        # keep record of every single trade we made
        # if this is the first non-cash position, we update the funding date (for pure convenience)
        if len(self.positions) <= 1:
            self.positions[CASH].time = as_of - timedelta(days=1)

        # update all current positions
        self.positions[CASH] += (cost, 1.0)
        self.positions[asset] = self.positions.get(
            asset, PortfolioPosition(strategy_id=self.strategy_id, asset=asset, time=as_of, quantity=0, cost_basis=0, value=quantity * price)
        ) + (quantity, price)

        # since we executed a trade for a given price we know exactly the price of the asset, and thus we
        # re-evaluate the portfolio.
        self.update_position_value(asset, as_of + timedelta(milliseconds=1), price, price)

        # Also since cash probably never gets a price we need to force cash evaluation as well
        self.update_position_value(CASH, as_of + timedelta(milliseconds=1), 1.0, 1.0)

    def update_position_value(self, asset, as_of, bid, ask):
        pos = self.positions.get(asset, None)
        if pos is None: return

        assert as_of >= pos.time, f"Can't back evaluate positions! {pos.time} > {as_of}"

        position_value = pos.quantity * ask if pos.quantity < 0 else pos.quantity * bid

        self.portfolio_history.append(
            pd.Series(
                dict(
                    strategy_id=self.strategy_id,
                    asset=asset,
                    time=as_of,
                    quantity=pos.quantity,
                    cost_basis=pos.cost_basis,
                    value=position_value
                ),
                name=as_of
            )
        )

    def get_portfolio_value(self, as_of: datetime | None = None) -> PortfolioValue:
        if as_of is None: as_of = datetime.max

        if as_of >= max([p.time for p in self.positions.values()]):
            # use what we have already in the object
            portfolio_value = sum([p.value for p in self.positions.values()])
            return PortfolioValue(
                self.positions[CASH].quantity,
                {p.asset: PositionValue(p.asset, p.quantity, p.value / portfolio_value, p.value) for p in self.positions.values()}
            )
        else:
            # select PortfolioHistory where time ceil(as_of)
            raise NotImplemented

    def get_portfolio_timeseries(self, as_of: datetime | None = None) -> pd.DataFrame:
        if as_of is None: as_of = datetime.max
        return pd.DataFrame(self.portfolio_history).sort_index()
