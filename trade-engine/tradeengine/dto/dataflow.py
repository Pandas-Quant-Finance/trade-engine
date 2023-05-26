from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Tuple

import numpy as np


@dataclass(frozen=True, eq=True)
class Asset:
    symbol: Any

    def __lt__(self, other):
        return self.symbol < other.symbol


@dataclass(frozen=True, eq=True)
class PositionValue:
    asset: Asset
    qty: float
    weight: float
    value: float


class _Position_addition(object):

    def add_quantity_and_price(_, self, other: Tuple[float, float]):
        self_quantity, self_cost_basis, self_pnl = self
        other_qty, other_price = other
        new_qty = self_quantity + other_qty
        pnl = 0

        if self_quantity > 0 and new_qty < self_quantity:
            new_cost_basis = self_cost_basis if new_qty >= 0 else other_price
            other_qty = min(-other_qty, self_quantity)
            pnl = (other_qty * other_price) - (other_qty * self_cost_basis)
        elif 0 < self_quantity < new_qty:
            new_cost_basis = (self_cost_basis * self_quantity + other_price * other_qty) / (self_quantity + other_qty)
        elif self_quantity < 0 and new_qty > self_quantity:
            new_cost_basis = self_cost_basis if new_qty <= 0 else other_price
            other_qty = min(other_qty, -self_quantity)
            pnl = (other_qty * self_cost_basis) + (-other_qty * other_price)
        elif 0 > self_quantity > new_qty:
            new_cost_basis = (self_cost_basis * self_quantity + other_price * other_qty) / (self_quantity + other_qty)
        else:
            new_cost_basis = other_price

        new_value = (new_qty * other_price)
        new_pnl = pnl + self_pnl

        return new_qty, new_cost_basis, new_value, new_pnl


@dataclass(frozen=False, eq=True)
class Position(_Position_addition):
    asset: Asset
    time: datetime
    quantity: float
    cost_basis: float
    value: float
    pnl: float = 0

    def __add__(self, other: Tuple[float, float]):
        new_qty, new_cost_basis, new_value, new_pnl = self.add_quantity_and_price((self.quantity, self.cost_basis, self.pnl), other)
        self.quantity = new_qty
        self.cost_basis = new_cost_basis
        self.value = new_value
        self.pnl = new_pnl
        return self

    def __sub__(self, other: Tuple[float, float]):
        return self + (-other[0], other[1])

    def __str__(self):
        return f"{self.asset}, {self.quantity}, {self.cost_basis}, {self.pnl}"


@dataclass(frozen=True, eq=True)
class PortfolioValue:
    cash: float
    positions: Dict[Asset, PositionValue]

    def value(self):
        return self.cash + sum([p.value for p in self.positions.values()])


@dataclass(frozen=True, eq=True)
class ExpectedExecutionPrice:
    time: datetime
    open_bid: float
    open_ask: float
    close_bid: float
    close_ask: float

    def evaluate_price(self, sign, order_time, limit):
        open, close = (self.open_bid if sign <= 0 else self.open_ask), (self.close_bid if sign <= 0 else self.close_ask)
        return (close if self.time > order_time else open) if limit is None else limit


class OrderTypes(Enum):
    CLOSE = 0
    QUANTITY = 1
    TARGET_QUANTITY = 2
    PERCENT = 3
    TARGET_WEIGHT = 4


@dataclass(frozen=True, eq=True)
class Order:
    asset: Asset
    size: float
    valid_from: datetime
    limit: float | None = None
    stop_limit: float | None = None
    valid_until: datetime = None
    id: int | None = None
    type = None

    def to_quantity(self, pv: PortfolioValue, price: ExpectedExecutionPrice) -> 'QuantityOrder':
        raise NotImplemented

    def _valid_until(self):
        # by default the order is only valid until the end of the trading day
        return self.valid_until or (self.valid_from + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


@dataclass(frozen=True, eq=True)
class QuantityOrder(Order):

    type = OrderTypes.QUANTITY

    def to_quantity(self, pv: PortfolioValue, price: ExpectedExecutionPrice) -> 'QuantityOrder':
        return self

    def __add__(self, other: 'QuantityOrder'):
        assert self.asset == other.asset, f"can not add orders of different assets {self.asset}, {other.asset}"
        return QuantityOrder(self.asset, self.size + other.size, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


@dataclass(frozen=True, eq=True)
class CloseOrder(Order):

    type = OrderTypes.CLOSE

    def to_quantity(self, pv: PortfolioValue, price: ExpectedExecutionPrice) -> 'QuantityOrder':
        q = -pv.positions[self.asset].qty if pv is not None and self.asset in pv.positions else 0
        return QuantityOrder(self.asset, q, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


@dataclass(frozen=True, eq=True)
class PercentOrder(Order):

    type = OrderTypes.PERCENT

    def to_quantity(self, pv: PortfolioValue, price: ExpectedExecutionPrice) -> 'QuantityOrder':
        # note that percent orders can only be positive and only executed with a positive balance
        price = price.evaluate_price(1, self.valid_from, self.limit)
        q = max(self.size, 0) * max(pv.cash, 0) / price if pv is not None else 0
        return QuantityOrder(self.asset, q, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


@dataclass(frozen=True, eq=True)
class TargetQuantityOrder(Order):

    type = OrderTypes.TARGET_QUANTITY

    def to_quantity(self, pv: PortfolioValue, price: ExpectedExecutionPrice) -> 'QuantityOrder':
        if pv is not None and self.asset in pv.positions:
            q = self.size - pv.positions[self.asset].qty
        else:
            q = self.size

        return QuantityOrder(self.asset, q, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


@dataclass(frozen=True, eq=True)
class TargetWeightOrder(Order):

    type = OrderTypes.TARGET_WEIGHT

    def to_quantity(self, pv: PortfolioValue, price: ExpectedExecutionPrice) -> 'QuantityOrder':
        if pv is not None and self.asset in pv.positions:
            w = self.size - pv.positions[self.asset].weight
        else:
            w = self.size

        price = price.evaluate_price(np.sign(w), self.valid_from, self.limit)
        return QuantityOrder(self.asset, (pv.value() * w) / price, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


CASH = Asset("$$$")
