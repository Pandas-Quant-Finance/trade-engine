from __future__ import annotations
import logging
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from threading import Lock
from typing import Dict, List, Tuple

import pandas as pd

from tradeengine.events import Asset, Position, Quote, TradeExecution
from .component import Component
from ..common.tz_compare import timestamp_greater
from .component import Component

_log = logging.getLogger(__name__)



class LatestQuote(Component):

    def __init__(self):
        super().__init__()
        self.lock = Lock()
        self.latest_quote: Dict[Asset, Tuple[datetime, float]] = {}

        self.register_event(Quote, handler=self.on_quote_update)

    def on_quote_update(self, quote: Quote):
        with self.lock:
            if quote.asset not in self.positions:
                return

            price = quote.get_price(0, 'last')

            # update latest quote
            if quote.asset in self.latest_quote:
                if timestamp_greater(self.latest_quote[quote.asset][0], quote.time):
                    _log.warning(
                        f"got obsolete quote from the past {quote.time} <= {self.latest_quote[quote.asset][0]}")
                    return

                self.latest_quote[quote.asset] = quote.time, price
            else:
                self.latest_quote[quote.asset] = quote.time, price

    # @handler(False)
    def get_latest_quote(self, asset: str | Asset) -> float:
        if isinstance(asset, str):
            asset = Asset(asset)

        with self.lock:
            return self.latest_quote[asset][1]
