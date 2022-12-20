from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


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
class Order:
    asset: Asset
    quantity: float
    limit: float | None = None
    valid_from: datetime | str = datetime.now()
    valid_to: datetime | str | int = None
    position_id: str = None

    def __post_init__(self):
        if self.position_id is None:
            self.position_id = self.asset.id
        if isinstance(self.valid_from, str):
            self.valid_from = datetime.fromisoformat(self.valid_from)
        if isinstance(self.valid_to, str):
            self.valid_to = datetime.fromisoformat(self.valid_to)

    def __sub__(self, other: int):
        return Order(self.asset, self.quantity, self.limit, self.valid_from, self.valid_to - other)


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
