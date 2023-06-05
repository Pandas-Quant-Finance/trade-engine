from dataclasses import dataclass
from typing import Tuple

from dataclasses_json import dataclass_json

from tradeengine.dto.asset import Asset


class PositionAdditionMixin(object):

    def add_quantity_and_price(self, other: Tuple[float, float]):
        self_quantity, self_cost_basis = self.quantity, self.cost_basis
        self_pnl = getattr(self, 'pnl') if hasattr(self, 'pnl') else 0

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


@dataclass_json
@dataclass(frozen=True, eq=True, init=False, repr=True)
class Position(PositionAdditionMixin):
    asset: Asset
    quantity: float
    cost_basis: float
    value: float
    pnl: float

    def __init__(self, asset: Asset, quantity: float, cost_basis: float = 1.0, value: float = None, pnl: float = 0):
        # mimic frozen dataclass constructor
        object.__setattr__(self, "asset", asset)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "cost_basis", cost_basis)
        object.__setattr__(self, "value", value if value is not None else (cost_basis * quantity))
        object.__setattr__(self, "pnl", pnl)

    def __add__(self, other: Tuple[float, float]):
        new_qty, new_cost_basis, new_value, new_pnl = self.add_quantity_and_price(other)
        return Position(self.asset, new_qty, new_cost_basis, new_value, new_pnl)

    def __sub__(self, other: Tuple[float, float]):
        return self + (-other[0], other[1])


@dataclass(frozen=True, eq=True, repr=True)
class PositionValue:
    asset: Asset
    qty: float
    weight: float
    value: float
