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

    def delta(self, pv: PositionValue):
        return 0


@dataclass(frozen=True, eq=True)
class CloseOrder(Order):
    pass


@dataclass(frozen=True, eq=True)
class QuantityOrder(Order):
    pass


@dataclass(frozen=True, eq=True)
class PercentOrder(Order):
    pass


@dataclass(frozen=True, eq=True)
class TargetQuantityOrder(Order):

    def delta(self, pv: PositionValue):
        #      10        - 12     => sell 2
        return self.size - pv.qty


@dataclass(frozen=True, eq=True)
class TargetWeightOrder(Order):

    def delta(self, pv: PositionValue):
        #      .01        - .012     => sell .002
        return self.size - pv.weight


