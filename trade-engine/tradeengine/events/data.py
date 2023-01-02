from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Tuple

import pandas as pd
from dateutil.tz import tzlocal


@dataclass(frozen=True, eq=True)
class Asset:
    id: str


class Position(object):

    def __init__(
            self, id: str | int | None,
            asset: Asset | str,
            quantity: float,
            price: float,
            **kwargs
    ):
        self.asset: Asset = asset if isinstance(asset, Asset) else Asset(asset)
        self.id: str = str(id) if id is not None else self.asset.id
        self.quantity: float = quantity
        self.change: float = kwargs.get("change", quantity)
        self.trade_price: float = kwargs.get("trade_price", price)
        self.cost_basis: float = price
        self.pnl: float = kwargs.get("pnl", 0)

    def evaluate(self, price: float, include_trade_delta: bool = True) -> Dict[str, Any]:
        p = self + (-self.quantity, price)
        return {
            "pid": p.id,
            "asset": p.asset.id,
            "trade": self.change if include_trade_delta else None,
            "cost_basis": self.cost_basis,
            "trade_price": self.trade_price if include_trade_delta else None,
            "position": self.quantity,
            "quote": price,
            "value": price * self.quantity,
            "unrealized_pnl": self.pnl - p.pnl,
            "realized_pnl": self.pnl,
            "pnl": 2 * self.pnl - p.pnl,
        }

    def __add__(self, other: Tuple[float, float]):
        other_qty, other_price = other
        new_qty = self.quantity + other_qty
        pnl = 0

        if self.quantity > 0 and new_qty < self.quantity:
            new_cost_basis = self.cost_basis if new_qty >= 0 else other_price
            other_qty = min(-other_qty, self.quantity)
            pnl = (other_qty * other_price) - (other_qty * self.cost_basis)
        elif 0 < self.quantity < new_qty:
            new_cost_basis = (self.cost_basis * self.quantity + other_price * other_qty) / (self.quantity + other_qty)
        elif self.quantity < 0 and new_qty > self.quantity:
            new_cost_basis = self.cost_basis if new_qty <= 0 else other_price
            other_qty = min(other_qty, -self.quantity)
            pnl = (other_qty * self.cost_basis) + (-other_qty * other_price)
        elif 0 > self.quantity > new_qty:
            new_cost_basis = (self.cost_basis * self.quantity + other_price * other_qty) / (self.quantity + other_qty)
        else:
            new_cost_basis = other_price

        return Position(
            self.id, self.asset, new_qty, new_cost_basis, pnl=pnl + self.pnl, change=other[0], trade_price=other_price
        )

    @property
    def value(self):
        return self.quantity * self.trade_price

    def __sub__(self, other: Tuple[float, float]):
        return self + (-other[0], other[1])

    def __eq__(self, o: object) -> bool:
        return (
            self.__class__ == o.__class__ and
            self.id == o.id and
            self.asset == o.asset
        )

    def __hash__(self) -> int:
        return hash(self.id) * 101 + hash(self.asset)

    def __str__(self):
        return f"{self.id}, {self.asset}, {self.quantity}, {self.cost_basis}, {self.pnl}"


class TargetWeights(object):

    def __init__(
            self,
            asset_weights: pd.Series | Dict[Asset, float] | Tuple[List[Asset], List[float]],
            valid_from: datetime | str = datetime.now(tz=tzlocal()),
            valid_to: datetime | str | int = None,
            position_id: str = ''
    ):
        if isinstance(asset_weights, pd.Series):
            self.asset_weights = asset_weights.to_dict()
        elif isinstance(asset_weights, tuple):
            self.asset_weights = dict(zip(*asset_weights))
        else:
            self.asset_weights = asset_weights

        self.asset_weights = {a if isinstance(a, Asset) else Asset(a): w for a, w in self.asset_weights.items()}
        self.valid_from: datetime = datetime.fromisoformat(valid_from) if isinstance(valid_from, str) else valid_from
        self.valid_to: datetime = datetime.fromisoformat(valid_to) if isinstance(valid_from, str) else valid_to
        self.position_id = position_id

        epsilon = 1e-5
        assert sum(self.asset_weights.values()) <= 1 + epsilon, f"Sum of weighs need to be <= 1.0 @ {self.valid_from}"
        assert sum(self.asset_weights.values()) >= -1 - epsilon, f"Sum of weighs need to be >= -1 @ {self.valid_from}"
        assert max(self.asset_weights.values()) <= 1 + epsilon, f"Max of weighs need to be <= 1.0 @ {self.valid_from}"
        assert min(self.asset_weights.values()) >= -1 - epsilon, f"Min of weighs need to be >= -1 @ {self.valid_from}"


class Order:

    def __init__(
            self,
            asset: Asset | str,
            quantity: float,
            limit: float | None = None,
            valid_from: datetime | str = datetime.now(tz=tzlocal()),
            valid_to: datetime | str | int | None = None,
            position_id: str | None = None
    ):
        self.asset: Asset = Asset(asset) if isinstance(asset, str) else asset
        self.quantity = quantity
        self.limit = limit
        self.valid_from: datetime = datetime.fromisoformat(valid_from) if isinstance(valid_from, str) else valid_from
        self.valid_to: datetime | int = datetime.fromisoformat(valid_to) if isinstance(valid_to, str) else valid_to
        self.position_id = position_id

    def valid_after_subtract_tick(self) -> bool:
        if isinstance(self.valid_to, int):
            self.valid_to -= 1
            return self.valid_to > 0
        else:
            return True


class MaximumOrder(Order):

    def __init__(
            self,
            asset: Asset | str,
            limit: float | None = None,
            valid_from: datetime | str = datetime.now(tz=tzlocal()),
            valid_to: datetime | str | int | None = None,
            position_id: str | None = None
    ):
        super().__init__(asset, float("NaN"), limit, valid_from, valid_to, position_id)


@dataclass
class CloseOrder(object):
    def __init__(
            self,
            position: Asset | str | None,
            limit: float | None = None,
            valid_from: datetime | str = datetime.now(tz=tzlocal())
    ):
        self.position = position
        self.limit = limit
        self.valid_from: datetime = datetime.fromisoformat(valid_from) if isinstance(valid_from, str) else valid_from


class BasketOrder(object):

    def __init__(
            self,
            assets: Dict[Asset | str, float | Tuple[float, float]],
            valid_from: datetime | str = datetime.now(tz=tzlocal()),
            valid_to: datetime | str | int | None = None,
            position_id: str | None = None
    ):
        pid = position_id if position_id else ''
        self.orders = [
            Order(
                a,
                ql[0] if isinstance(ql, tuple) else ql,
                ql[1] if isinstance(ql, tuple) else None,
                valid_from,
                valid_to,
                pid
            ) for a, ql in assets.items()
        ]


@dataclass
class BidAsk:
    bid: float
    ask: float
    bid_volume: float | None = None
    ask_volume: float | None = None


@dataclass
class Bar:
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


class Quote(object):

    def __init__(
            self,
            asset: Asset | str,
            time: datetime | str,
            price: float | BidAsk | Bar
    ):
        self.asset: Asset = Asset(asset) if isinstance(asset, str) else asset
        self.time: datetime = datetime.fromisoformat(time) if isinstance(time, str) else time
        self.price = price

    def get_price(self, quantity: float, limit: float | str = None, slippage: float = 0):
        assert slippage >= 0, "slippage can't be negative"

        pricing = self.price
        slippage_factor = (1 + slippage) if quantity > 0 else (1 - slippage)

        if isinstance(pricing, (float, int)):
            if limit is not None and not isinstance(limit, str):
                if quantity > 0 and limit < pricing:
                    return None
                elif quantity < 0 and limit > pricing:
                    return None
                else:
                    return pricing * slippage_factor
            else:
                return pricing * slippage_factor
        elif isinstance(pricing, BidAsk):
            if quantity > 0:
                if limit is not None and not isinstance(limit, str) and limit < pricing.ask:
                    return None
                else:
                    return pricing.ask * slippage_factor
            elif quantity < 0:
                if limit is not None and not isinstance(limit, str) and limit > pricing.bid:
                    return None
                else:
                    return pricing.bid * slippage_factor
            else:
                return (pricing.bid + pricing.ask) / 2  * slippage_factor
        elif isinstance(pricing, Bar):
            if limit is None:
                return pricing.open * slippage_factor
            elif quantity == 0:
                return pricing.close * slippage_factor
            elif limit == 'last':
                return pricing.close * slippage_factor
            else:
                if quantity > 0:
                    if limit < pricing.low:
                        return None
                    else:
                        return min(pricing.open, limit) * slippage_factor
                elif quantity < 0:
                    if limit > pricing.high:
                        return None
                    else:
                        return max(pricing.open, limit) * slippage_factor
                else:
                    return limit * slippage_factor
        else:
            raise ValueError(f"Unknown quoting {type(pricing)}")


@dataclass
class TradeExecution:
    asset: Asset
    quantity: float
    price: float
    time: datetime
    quote: Quote
    position_id: str = None

    def __post_init__(self):
        if self.position_id is None:
            self.position_id = self.asset.id



class TickMarketDataClock(object):

    def __init__(self, asset: Asset, time: datetime | str):
        self.asset: Asset = asset
        self.time: datetime = datetime.fromisoformat(time) if isinstance(time, str) else time


@dataclass
class SubscribeToMarketData:
    asset: Asset
    time: datetime


@dataclass
class CancelOrder:
    order: Order

