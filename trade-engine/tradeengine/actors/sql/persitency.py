from datetime import datetime
from enum import Enum
from typing import Tuple

from sqlalchemy import ForeignKey, String, DateTime, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, composite

from tradeengine.dto.dataflow import Asset


# objects for SQL Alchemy
class OrderBookBase(DeclarativeBase):
    pass

class OrderBook(OrderBookBase):
    __tablename__ = 'orderbook'
    asset_id: Mapped[str] = mapped_column(primary_key=True)
    order_type: Mapped[Enum]



class PortfolioBase(DeclarativeBase):
    pass


class PortfolioTrade(PortfolioBase):
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


class PortfolioPosition(PortfolioBase):
    __tablename__ = 'portfolio_position'
    strategy_id: Mapped[str] = mapped_column(primary_key=True)
    asset: Mapped[Asset] = composite(mapped_column(String(255), primary_key=True))
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    quantity: Mapped[float] = mapped_column()
    cost_basis: Mapped[float] = mapped_column()
    value: Mapped[float] = mapped_column()

    def __add__(self, other: Tuple[float, float]):
        other_qty, other_price = other
        new_qty = self.quantity + other_qty

        if self.quantity > 0 and new_qty < self.quantity:
            new_cost_basis = self.cost_basis if new_qty >= 0 else other_price
        elif 0 < self.quantity < new_qty:
            new_cost_basis = (self.cost_basis * self.quantity + other_price * other_qty) / (self.quantity + other_qty)
        elif self.quantity < 0 and new_qty > self.quantity:
            new_cost_basis = self.cost_basis if new_qty <= 0 else other_price
        elif 0 > self.quantity > new_qty:
            new_cost_basis = (self.cost_basis * self.quantity + other_price * other_qty) / (self.quantity + other_qty)
        else:
            new_cost_basis = other_price

        self.quantity = new_qty
        self.cost_basis = new_cost_basis
        self.value = (new_qty * other_price)
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