from dataclasses import dataclass
from typing import Dict

from tradeengine.dto.asset import Asset
from tradeengine.dto.position import PositionValue


@dataclass(frozen=True, eq=True, repr=True)
class PortfolioValue:
    cash: float
    positions: Dict[Asset, PositionValue]

    def value(self):
        # NOTE that cash is also a position! so we don't need to self.cash + ...
        return sum([p.value for p in self.positions.values()])
