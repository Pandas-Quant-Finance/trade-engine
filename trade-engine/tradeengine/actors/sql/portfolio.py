from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple

import pandas as pd
from sqlalchemy import Engine, text, select, func, update
from sqlalchemy.orm import Session
from tradeengine.actors.portfolio_actor import AbstractPortfolioActor
from tradeengine.actors.sql.persitency import PortfolioBase, PortfolioTrade, PortfolioHistory, PortfolioPosition
from tradeengine.dto.dataflow import PositionValue, PortfolioValue, Asset

LOG = logging.getLogger(__name__)
CASH = Asset("$$$")
FUNDING_DATE = datetime.utcnow().replace(year=1900, month=1, day=1)


# TODO the PortfolioTrade is actually not needed at all and should be removed, if needed we could embed the delta data into the PortfolioHistory
# TODO the PortfolioHistory should become it's own actor and we just tell him about recording history objects

class SQLPortfolioActor(AbstractPortfolioActor):

    def __init__(
            self,
            alchemy_engine: Engine,
            funding: float = 1.0,
            strategy_id: str = '',
            funding_date: datetime = FUNDING_DATE
    ):
        super().__init__(funding)
        self.alchemy_engine = alchemy_engine
        self.strategy_id = strategy_id
        self.positions: Dict[Asset, PortfolioPosition] = {}
        self.funding_date = funding_date

        PortfolioBase.metadata.create_all(bind=alchemy_engine)
        session = self.session = Session(self.alchemy_engine, expire_on_commit=False)

        # get most recent positions
        for pp in session.scalars(select(PortfolioPosition).where(PortfolioPosition.strategy_id == self.strategy_id)):
            self.positions[pp.asset] = pp

        # in case we have an empty portfolio initialize the cash position
        if len(self.positions) <= 0:
            initial_portfolio = [
                PortfolioTrade(strategy_id=self.strategy_id, asset=CASH, time=funding_date, quantity=funding, cost=1.0),
                PortfolioPosition(strategy_id=self.strategy_id, asset=CASH, time=funding_date, quantity=funding, cost_basis=1.0, value=funding),
            ]

            session.add_all(initial_portfolio)
            session.commit()

            self.positions[initial_portfolio[-1].asset] = initial_portfolio[-1]
            self.update_position_value(CASH, funding_date, 1.0, 1.0)

    def on_stop(self) -> None:
        try:
            # close database connection
            self.session.commit()
            self.alchemy_engine.dispose()
        except Exception as e:
            LOG.error(e)

    def add_new_position(self, asset, as_of, quantity, price, fee):
        assert as_of > self.funding_date, f"can't add trades before the portfolio was funded! {as_of} > {self.funding_date}"
        assert as_of >= self.positions.get(asset, PortfolioPosition(time=as_of)).time, \
            f"Can't backdate positions! {self.positions.get(asset, PortfolioPosition(time=as_of)).time} > {as_of}"

        # every trade as a cost aspect as in cash
        cost = -quantity * price - fee

        # keep record of every single trade we made, we don't want these guys to stick around in some session memory
        # maybe there is a better way then opening a new session for this.
        with Session(self.alchemy_engine) as session:
            session.add_all([
                PortfolioTrade(strategy_id=self.strategy_id, asset=asset, time=as_of, quantity=quantity, cost=price),
                PortfolioTrade(strategy_id=self.strategy_id, asset=CASH, time=as_of, quantity=cost, cost=1.0),
            ])

            # if this is the first non-cash position, we update the funding date (for pure convenience)
            if len(self.positions) <= 1:
                self.positions[CASH].time = as_of - timedelta(days=1)
                session.execute(
                    update(PortfolioHistory)\
                        .where((PortfolioHistory.strategy_id == self.strategy_id) & (PortfolioHistory.time == self.funding_date) & (PortfolioHistory.asset == CASH))\
                        .values({PortfolioHistory.time: as_of - timedelta(days=1)})
                )

            session.commit()

        # update all current positions
        self.positions[CASH] += (cost, 1.0)
        self.positions[asset] = self.positions.get(
            asset, PortfolioPosition(strategy_id=self.strategy_id, asset=asset, time=as_of, quantity=0, cost_basis=0, value=quantity * price)
        ) + (quantity, price)

        self.session.add_all([self.positions[asset], self.positions[CASH]])
        self.session.commit()

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

        # we don't want these guys to stick around in some session memory
        # maybe there is a better way then opening a new session for this.
        with Session(self.alchemy_engine) as session:
            session.add(
                PortfolioHistory(
                    strategy_id=self.strategy_id,
                    asset=asset,
                    time=as_of,
                    quantity=pos.quantity,
                    cost_basis=pos.cost_basis,
                    value=position_value
                )
            )
            session.commit()

        # update portfolio position value
        pos.value = position_value
        self.session.add(pos)
        self.session.commit()

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

        with Session(self.alchemy_engine) as session:
            return pd.DataFrame(
                [
                    h.to_dict() for h in
                        session.scalars(
                            select(PortfolioHistory)\
                                .where((PortfolioHistory.strategy_id == self.strategy_id) & (PortfolioHistory.time <= as_of))\
                                .order_by(PortfolioHistory.time)
                        )
                ]
            )

    # TODO derelease this function ..
    def get_trades(self, as_of: datetime | None = None) -> pd.DataFrame:
        if as_of is None: as_of = datetime.max

        with Session(self.alchemy_engine) as session:
            return pd.DataFrame(
                [
                    pp.to_dict() for pp in
                        session.scalars(
                            select(PortfolioTrade)\
                                .where((PortfolioTrade.strategy_id == self.strategy_id) & (PortfolioTrade.time <= as_of))\
                                .order_by(PortfolioTrade.time)
                        )
                ]
            )

