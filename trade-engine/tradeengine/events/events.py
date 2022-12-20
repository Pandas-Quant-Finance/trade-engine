from __future__ import annotations

from typing import List

from circuits import Event

from .data import *


class TakePoisonPillEvent(Event):
    """ poison pill kills app """


class ReadyForComplexTradeEvent(Event):
    """ just a notification that we want to initiate trade """

    def __init__(self, assets: List[Asset], time: datetime):
        super().__init__(time)


class SubmitTradeEvent(Event):

    def __init__(self, order: Order):
        super().__init__(order)


class TradeExecutedEvent(Event):

    def __init__(self, trade: TradeExecution):
        super().__init__(trade)


class QuoteUpdatedEvent(Event):

    def __init__(self, quote: Quote):
        super().__init__(quote)


class SubscribeToQuoteProviderEvent(Event):

    def __init__(self, asset: Asset, time: datetime = datetime.now()):
        super().__init__(asset, time)


class NewTickEvent(Event):

    def __init__(self, tick: datetime):
        super().__init__(tick)
