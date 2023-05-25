from datetime import datetime
from typing import Tuple

from sqlalchemy import ForeignKey, String, DateTime, Integer, Enum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, composite

from tradeengine.dto.dataflow import Asset, OrderTypes, _Position_addition


# objects for SQL Alchemy
class OrderBookBase(DeclarativeBase):

    def __repr__(self):
        params = ', '.join(f'{k}={v}' for k, v in self.__dict__.items() if not k.startswith("_"))
        return f'{self.__class__.__name__}({params})'


class OrderBook(OrderBookBase):
    __tablename__ = 'orderbook'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    strategy_id: Mapped[str] = mapped_column(index=True)
    order_type: Mapped[OrderTypes] = mapped_column(Enum(OrderTypes, length=50), index=True)
    asset: Mapped[Asset] = composite(mapped_column(String(255), index=True))
    limit:  Mapped[float] = mapped_column(nullable=True)
    stop_limit: Mapped[float] = mapped_column(nullable=True)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    qty: Mapped[float] = mapped_column(nullable=True)

    def to_history(self, status, filled):
        return OrderBookHistory(
            strategy_id=self.strategy_id,
            order_type=self.order_type,
            asset=self.asset,
            limit=self.limit,
            stop_limit=self.stop_limit,
            valid_from=self.valid_from,
            valid_until=self.valid_until,
            qty=self.qty,
            status=status,
            filled=filled
        )


class OrderBookHistory(OrderBookBase):
    __tablename__ = 'orderbook_history'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    strategy_id: Mapped[str] = mapped_column(index=True)
    order_type: Mapped[OrderTypes] = mapped_column(Enum(OrderTypes, length=50), index=True)
    asset: Mapped[Asset] = composite(mapped_column(String(255), index=True))
    limit: Mapped[float] = mapped_column(nullable=True)
    stop_limit: Mapped[float] = mapped_column(nullable=True)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    qty: Mapped[float] = mapped_column(nullable=True)
    status: Mapped[int] = mapped_column()
    filled: Mapped[float] = mapped_column(nullable=True)


class PortfolioBase(DeclarativeBase):

    def __repr__(self):
        params = ', '.join(f'{k}={v}' for k, v in self.__dict__.items() if not k.startswith("_"))
        return f'{self.__class__.__name__}({params})'


class PortfolioTrade(PortfolioBase):
    # FIXME this class is obsolete
    __tablename__ = 'portfolio_trade'
    strategy_id: Mapped[str] = mapped_column(primary_key=True)
    asset: Mapped[Asset] = composite(mapped_column(String(255), primary_key=True))
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    quantity: Mapped[float] = mapped_column()
    cost: Mapped[float] = mapped_column()

    def to_dict(self):
        return dict(
            strategy_id=self.strategy_id,
            asset=self.asset,
            time=self.time,
            quantity=self.quantity,
            cost=self.cost,
        )


class PortfolioPosition(PortfolioBase, _Position_addition):
    __tablename__ = 'portfolio_position'
    strategy_id: Mapped[str] = mapped_column(primary_key=True)
    asset: Mapped[Asset] = composite(mapped_column(String(255), primary_key=True))
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    quantity: Mapped[float] = mapped_column()
    cost_basis: Mapped[float] = mapped_column()
    value: Mapped[float] = mapped_column()

    def __add__(self, other: Tuple[float, float]):
        new_qty, new_cost_basis, new_value, new_pnl = self.add_quantity_and_price((self.quantity, self.cost_basis, 0), other)
        self.quantity = new_qty
        self.cost_basis = new_cost_basis
        self.value = new_value
        return self

    def __sub__(self, other: Tuple[float, float]):
        return self + (-other[0], other[1])


class PortfolioHistory(PortfolioBase):
    __tablename__ = 'portfolio_history'
    strategy_id: Mapped[str] = mapped_column(primary_key=True)
    asset: Mapped[Asset] = composite(mapped_column(String(255), primary_key=True))
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    quantity: Mapped[float] = mapped_column()
    cost_basis: Mapped[float] = mapped_column()
    value: Mapped[float] = mapped_column()

    def to_dict(self):
        return dict(
            strategy_id=self.strategy_id,
            asset=self.asset,
            time=self.time,
            quantity=self.quantity,
            cost_basis=self.cost_basis,
            value=self.value,
        )
