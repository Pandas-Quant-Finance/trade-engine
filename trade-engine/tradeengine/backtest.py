import uuid
from datetime import timedelta
from typing import Dict, List

import pandas as pd

from tradeengine.actors.memory import PandasQuoteProviderActor
from tradeengine.dto.dataflow import Asset


def backtest_strategy(
        market_data: Dict[Asset, pd.DataFrame],
        # signal, something to create an Order Message Object
        market_data_price_columns: List = ("Open", "High", "Low", "Close"),
        market_data_interval: timedelta = timedelta(seconds=1),
        strategy_id: str = str(uuid.uuid4()),
):
    portfolio_actor = None
    orderbook_actor = None
    market_data_actor = PandasQuoteProviderActor.start(portfolio_actor, orderbook_actor, market_data, market_data_price_columns)

    pass
