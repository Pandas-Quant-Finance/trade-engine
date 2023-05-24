from __future__ import annotations
import logging
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from threading import Lock
from typing import Dict, List

import pandas as pd

from tradeengine._obsolete.events import Asset, Position, Quote, TradeExecution
from .component import Component

_log = logging.getLogger(__name__)


class Portfolio(Component):

    def __init__(self):
        super().__init__()
        self.lock = Lock()
        self.positions: Dict[Asset, Dict[str, Position]] = defaultdict(dict)
        self.timeseries: Dict[str, Dict[datetime, dict]] = defaultdict(dict)
        self.position_value: Dict[str, float] = defaultdict(lambda: 0)

        self.register_event(Quote, handler=self.on_quote_update)
        self.register_event(TradeExecution, handler=self.on_trade_execution)

    def on_quote_update(self, quote: Quote):
        with self.lock:
            if quote.asset not in self.positions:
                return

            price = quote.get_price(0, 'last')

            # update position timeseries
            for pid, pos in self.positions[quote.asset].items():
                val_pos = pos.evaluate(price, include_trade_delta=False)
                self.position_value[pid] = val_pos["value"]
                if quote.time not in self.timeseries[pid]:
                    self.timeseries[pid][quote.time] = val_pos

    def on_trade_execution(self, trade: TradeExecution):
        _log.debug(f"got new trade execution {trade}")
        with self.lock:
            if trade.position_id in self.positions[trade.asset]:
                self.positions[trade.asset][trade.position_id] += (trade.quantity, trade.price)
            else:
                self.positions[trade.asset][trade.position_id] = \
                    Position(trade.position_id, trade.asset, trade.quantity, trade.price)

            # this also is most likely the latest quote we are aware of, so we kep track of
            #  the latest quote
            #  the latest position value
            #  start an entry in the timeseries
            pos_val = self.positions[trade.asset][trade.position_id].evaluate(trade.quote.get_price(0, 'last'), include_trade_delta=True)
            self.position_value[trade.position_id] = pos_val["value"]
            self.timeseries[trade.position_id][trade.time] = pos_val

    def get_timeseries(self):
        # if we find ourselves to call this method very ofter we could think about cashing the data frame
        # and just clear out the timeseries hashmaps (Dict[str, Dict[datetime, dict]]) and concatenate the results

        with self.lock:
            df_ts = pd.concat(
                [pd.DataFrame(ts.values(), index=ts.keys()) for ts in self.timeseries.values()],
                axis=1,
                keys=self.timeseries.keys(),
                sort=True
            ).swaplevel(0, 1, axis=1)

            return df_ts.swaplevel(0, 1, axis=1)

    def get_quantity(self, asset: Asset | str, pid=None):
        if not isinstance(asset, Asset):
            asset = Asset(asset)

        with self.lock:
            if not asset in self.positions:
                return 0

            if pid is None:
                return sum([p.quantity for p in self.positions[asset].values()])

            if not pid in self.positions[asset]:
                return 0

            return self.positions[asset][pid].quantity

    @property
    def total_position_value(self):
        with self.lock:
            return sum(self.position_value.values())

    @property
    def assets(self) -> List[Asset]:
        with self.lock:
            return list(self.positions.keys())

    def get_positions(self) -> Dict[Asset, Dict[str, Position]]:
        with self.lock:
            return deepcopy(self.positions)
