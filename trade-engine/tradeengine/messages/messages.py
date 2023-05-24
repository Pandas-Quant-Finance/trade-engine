from dataclasses import dataclass
from datetime import datetime

from tradeengine.dto.dataflow import Order, Asset


@dataclass(frozen=True, eq=True)
class Message:
    pass


@dataclass(frozen=True, eq=True)
class PortfolioValueMessage(Message):
    as_of: datetime | None = None


@dataclass(frozen=True, eq=True)
class PortfolioTradesMessage(Message):
    as_of: datetime | None = None


@dataclass(frozen=True, eq=True)
class PortfolioPerformanceMessage(Message):
    as_of: datetime | None = None
    resample_rule: str | None = None


@dataclass(frozen=True, eq=True)
class NewPositionMessage(Message):
    asset: Asset
    as_of: datetime
    quantity: float
    price: float
    fee: float


@dataclass(frozen=True, eq=True)
class ReplayAllMarketDataMessage(Message):
    pass


@dataclass(frozen=True, eq=True)
class NewMarketDataMessage(Message):
    asset: Asset
    as_of: datetime


@dataclass(frozen=True, eq=True)
class NewBidAskMarketData(NewMarketDataMessage):
    bid: float
    ask: float


@dataclass(frozen=True, eq=True)
class NewBarMarketData(NewMarketDataMessage):
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True, eq=True)
class NewOrderMessage(Message):
    order: Order








