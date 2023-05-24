import uuid
from datetime import timedelta
from typing import Dict, List, Iterable, Type, Any

import pandas as pd
import pykka

from tradeengine.actors.memory import PandasQuoteProviderActor
from tradeengine.dto.dataflow import Asset, Order
from tradeengine.messages import NewOrderMessage


def backtest_strategy(
        orderbook_actor: pykka.ActorRef,
        portfolio_actor: pykka.ActorRef,
        market_data: Dict[Asset, pd.DataFrame],
        signal: Dict[Asset, pd.Series],  # pass a series of [pd.Timestamp, Dict[Type[Order], Dict[str, Any]]]]
        market_data_price_columns: List = ("Open", "High", "Low", "Close"),
        market_data_interval: timedelta = timedelta(seconds=1),
):
    if not isinstance(market_data_price_columns, list):
        market_data_price_columns = list(market_data_price_columns)

    try:
        market_data_actor = PandasQuoteProviderActor.start(portfolio_actor, orderbook_actor, market_data, market_data_price_columns)

        order_futures = []
        for asset, signals in signal.items():
            # signal needs to be Dict[Type[Order], Dict[str, Any]]
            for tst, signal in signals.items():
                if signal is None:
                    continue

                if isinstance(tst, pd.Timestamp):
                    tst = tst.to_pydatetime()

                for order_type, order_args in signal.items():
                    # prevent lookahead bias!!
                    valid_from = tst + market_data_interval

                    # convert any dates to pydates
                    for oa, val in order_args.items():
                        if isinstance(val, pd.Timestamp):
                            order_args[oa] = val.to_pydatetime()

                    # finally we can send of the order to he orderbook actor
                    order_futures.append(
                        orderbook_actor.ask(NewOrderMessage(order_type(asset, **{**order_args, "valid_from": valid_from})), block=False)
                    )

        # before we send of market data and trigger the whole system to simulate order execution and portfolio valuation
        # we wait until all orders are placed
        _ = [f.get() for f in order_futures]

        print(orderbook_actor.proxy().get_full_orderbook().get())
    finally:
        pykka.ActorRegistry.stop_all()

