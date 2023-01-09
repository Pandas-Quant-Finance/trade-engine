from __future__ import annotations

import logging
from datetime import datetime

import yfinance

from ..events import Bar

_log = logging.getLogger(__name__)

from .backtester import PandasBarBacktester


class YfBacktester(PandasBarBacktester):

    def __init__(
            self,
            starting_date: datetime | str,
            starting_balance: float = 100,
            slippage: float = 0,
            derive_quantity_slippage: float = 0.02,
            order_minimum_quantity: float = 1e-4,
            min_target_weight: float = 1e-4,
            autostart: bool = True
    ):
        super().__init__(
            lambda asset, time: yfinance.Ticker(asset.id).history(start=time),
            lambda row: Bar(row["Open"], row["High"], row["Low"], row["Close"], ),
            starting_date,
            starting_balance,
            slippage,
            derive_quantity_slippage,
            order_minimum_quantity,
            min_target_weight,
            autostart
        )
