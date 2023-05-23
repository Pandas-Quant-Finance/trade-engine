from dataclasses import dataclass
from datetime import datetime

import pykka

from tradeengine.events import Asset


@dataclass(frozen=True, eq=True)
class Message:
    pass

#@dataclass(frozen=True, eq=True)
#class NewMarketDataProviderMessage(Message):
#    provider: pykka.ActorRef


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
class QuoteAskMarketData(NewMarketDataMessage):
    quote: float


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
class OrderMessage(Message):
    asset: Asset
    limit: float | None
    stop_limit: float | None
    valid_from: datetime
    valid_until: datetime


@dataclass(frozen=True, eq=True)
class CloseOrderMessage(OrderMessage):
    pass


@dataclass(frozen=True, eq=True)
class QuantityOrderMessage(OrderMessage):
    qty: float


@dataclass(frozen=True, eq=True)
class TargetQuantityOrderMessage(OrderMessage):
    qty: float


@dataclass(frozen=True, eq=True)
class PercentOrderMessage(OrderMessage):
    percent: float


@dataclass(frozen=True, eq=True)
class TargetWeightOrderMessage(OrderMessage):
    weight: float









