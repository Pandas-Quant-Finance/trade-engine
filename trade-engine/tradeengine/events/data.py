from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Tuple

import pandas as pd


@dataclass(frozen=True, eq=True)
class Asset:
    id: str


@dataclass
class Position:
    id: str
    asset: Asset
    quantity: float = field(hash=False, compare=False)

    def __add__(self, other: float):
        return Position(self.id, self.asset, self.quantity + other)

    def __sub__(self, other: float):
        return Position(self.id, self.asset, self.quantity - other)


@dataclass
class TargetWeights:
    asset_weights: pd.Series | Dict[Asset, float] | Tuple[List[Asset], List[float]]
    valid_from: datetime | str = datetime.now()
    valid_to: datetime | str | int = None
    position_id: str = ''

    def __post_init__(self):
        if isinstance(self.asset_weights, pd.Series):
            self.asset_weights = ([Asset(*a) for a in self.asset_weights.index], self.asset_weights.values.tolist())
        elif isinstance(self.asset_weights, dict):
            self.asset_weights = (list(self.asset_weights.keys()), list(self.asset_weights.values()))

        epsilon = 1e-5
        assert sum(self.asset_weights[1]) <= 1 + epsilon, f"Sum of weighs need to be <= 1.0 @ {self.valid_from}"
        assert sum(self.asset_weights[1]) >= -1 - epsilon, f"Sum of weighs need to be >= -1 @ {self.valid_from}"
        assert max(self.asset_weights[1]) <= 1 + epsilon, f"Max of weighs need to be <= 1.0 @ {self.valid_from}"
        assert min(self.asset_weights[1]) >= -1 - epsilon, f"Min of weighs need to be >= -1 @ {self.valid_from}"

        # fix dates
        if isinstance(self.valid_from, str):
            self.valid_from = datetime.fromisoformat(self.valid_from)
        if isinstance(self.valid_to, str):
            self.valid_to = datetime.fromisoformat(self.valid_to)


@dataclass
class Order:
    asset: Asset | List[Asset]
    quantity: float | List[float]
    limit: float | List[float] | None = None
    valid_from: datetime | str = datetime.now()
    valid_to: datetime | str | int = None
    position_id: str = None

    def __post_init__(self):
        if self.position_id is None:
            self.position_id = self.asset.id if not isinstance(self.asset, list) else ''
        if isinstance(self.valid_from, str):
            self.valid_from = datetime.fromisoformat(self.valid_from)
        if isinstance(self.valid_to, str):
            self.valid_to = datetime.fromisoformat(self.valid_to)

    def __add__(self, other: int):
        return Order(self.asset, self.quantity, self.limit, self.valid_from, self.valid_to + other, self.position_id)

    def __sub__(self, other: int):
        return Order(self.asset, self.quantity, self.limit, self.valid_from, self.valid_to - other, self.position_id)

    def tz_aware(self, reference_date: datetime | pd.Timestamp):
        if self.valid_from.tzinfo is None:
            valid_from = pd.Timestamp(self.valid_from, tz=reference_date.tzinfo)
        else:
            valid_from = self.valid_from

        if self.valid_to is not None and not isinstance(self.valid_to, (int, float)) and self.valid_to.tzinfo is None:
            valid_to = pd.Timestamp(self.valid_to, tz=reference_date.tzinfo)
        else:
            valid_to = self.valid_to

        return Order(self.asset, self.quantity, self.limit, valid_from, valid_to, self.position_id)

    def order_at_index(self, i):
        return Order(
            self.asset[i], self.quantity[i], self.limit,
            self.valid_from, self.valid_to,
            (str(self.position_id) or '') + '/' + str(self.asset[i].id)
        )


@dataclass
class TradeExecution:
    asset: Asset
    quantity: float
    price: float
    position_id: str = None

    def __post_init__(self):
        if self.position_id is None:
            self.position_id = self.asset.id


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


@dataclass
class Quote:
    asset: Asset
    time: datetime
    price: float | BidAsk | Bar


@dataclass
class PositionTimeSeries:
    pid: str
    time: datetime
    quantity: float
    price: float
    value: float = None

    def __post_init__(self):
        if self.value is None:
            self.value = self.quantity * self.price
