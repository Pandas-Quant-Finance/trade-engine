from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, List, Dict

import pandas as pd


@dataclass(frozen=True, eq=True)
class Asset:
    symbol: Any


@dataclass(frozen=True, eq=True)
class PositionValue:
    asset: Asset
    qty: float
    weight: float
    value: float


@dataclass(frozen=True, eq=True)
class PortfolioValue:
    cash: float
    positions: Dict[Asset, PositionValue]

    def value(self):
        return sum([p.value for p in self.positions.values()])


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

    def to_quantity(self, pv: PortfolioValue, price: float) -> 'QuantityOrder':
        raise NotImplemented

    def _valid_until(self):
        # by default the order is only valid until the end of the trading day
        return self.valid_until or (self.valid_from + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


@dataclass(frozen=True, eq=True)
class QuantityOrder(Order):

    type = OrderTypes.QUANTITY

    def to_quantity(self, pv: PortfolioValue, price: float) -> 'QuantityOrder':
        return self

    def __add__(self, other: 'QuantityOrder'):
        assert self.asset == other.asset, f"can not add orders of different assets {self.asset}, {other.asset}"
        return QuantityOrder(self.asset, self.size + other.size, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


@dataclass(frozen=True, eq=True)
class CloseOrder(Order):

    type = OrderTypes.CLOSE

    def to_quantity(self, pv: PortfolioValue, price: float) -> QuantityOrder:
        q = -pv.positions[self.asset].qty if pv is not None and self.asset in pv.positions else 0
        return QuantityOrder(self.asset, q, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


@dataclass(frozen=True, eq=True)
class PercentOrder(Order):

    type = OrderTypes.PERCENT

    def to_quantity(self, pv: PortfolioValue, price: float) -> QuantityOrder:
        # note that percent orders can only be positive and only executed with a positive balance
        q = max(self.size, 0) * max(pv.cash, 0) / price if pv is not None else 0
        return QuantityOrder(self.asset, q, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


@dataclass(frozen=True, eq=True)
class TargetQuantityOrder(Order):

    type = OrderTypes.TARGET_QUANTITY

    def to_quantity(self, pv: PortfolioValue, price: float) -> QuantityOrder:
        if pv is not None and self.asset in pv.positions:
            q = self.size - pv.positions[self.asset].qty
        else:
            q = self.size

        return QuantityOrder(self.asset, q, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)


@dataclass(frozen=True, eq=True)
class TargetWeightOrder(Order):

    type = OrderTypes.TARGET_WEIGHT

    def to_quantity(self, pv: PortfolioValue, price: float) -> QuantityOrder:
        if pv is not None and self.asset in pv.positions:
            w = self.size - pv.positions[self.asset].weight
        else:
            w = self.size

        return QuantityOrder(self.asset, (pv.value() * w) / price, self.valid_from, self.limit, self.stop_limit, self.valid_until, self.id)

