from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Dict


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
    id: Any
    asset: Asset
    size: float
    type = None

    def to_quantity(self, pv: PortfolioValue, price: float) -> 'QuantityOrder':
        raise NotImplemented


@dataclass(frozen=True, eq=True)
class QuantityOrder(Order):

    type = OrderTypes.QUANTITY

    def to_quantity(self, pv: PortfolioValue, price: float) -> 'QuantityOrder':
        return self

    def __add__(self, other: 'QuantityOrder'):
        assert self.asset == other.asset, f"can not add orders of different assets {self.asset}, {other.asset}"
        return QuantityOrder(self.asset, self.size + other.size)


@dataclass(frozen=True, eq=True)
class CloseOrder(Order):

    type = OrderTypes.CLOSE

    def to_quantity(self, pv: PortfolioValue, price: float) -> QuantityOrder:
        return QuantityOrder(self.asset, -pv.positions[self.asset].qty)


@dataclass(frozen=True, eq=True)
class PercentOrder(Order):

    type = OrderTypes.PERCENT

    def to_quantity(self, pv: PortfolioValue, price: float) -> QuantityOrder:
        return QuantityOrder(self.asset, self.size * pv.cash / price)


@dataclass(frozen=True, eq=True)
class TargetQuantityOrder(Order):

    type = OrderTypes.TARGET_QUANTITY

    def to_quantity(self, pv: PortfolioValue, price: float) -> QuantityOrder:
        return QuantityOrder(self.asset, self.size - pv.positions[self.asset].qty)


@dataclass(frozen=True, eq=True)
class TargetWeightOrder(Order):

    type = OrderTypes.TARGET_WEIGHT

    def to_quantity(self, pv: PortfolioValue, price: float) -> QuantityOrder:
        return QuantityOrder(self.asset, (pv.value() * (self.size - pv.positions[self.asset].weight)) / price)


