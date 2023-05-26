import logging
import uuid
from datetime import timedelta
from typing import Dict, List, Iterable, Type, Any

import pandas as pd
import pykka

from tradeengine.actors.memory import PandasQuoteProviderActor
from tradeengine.dto.dataflow import Asset, Order
from tradeengine.messages import NewOrderMessage, ReplayAllMarketDataMessage, PortfolioPerformanceMessage, \
    AllExecutedOrderHistory

LOG = logging.getLogger(__name__)


def backtest_strategy(
        orderbook_actor: pykka.ActorRef,
        portfolio_actor: pykka.ActorRef,
        market_data: Dict[Asset, pd.DataFrame],
        signal: Dict[Asset, pd.Series],  # pass a series of [pd.Timestamp, Dict[Type[Order], Dict[str, Any]]]]
        market_data_price_columns: List = ("Open", "High", "Low", "Close"),
        market_data_interval: timedelta = timedelta(seconds=1),
        resample_rule='D',
        shutdown_on_complete: bool = True,
):
    if not isinstance(market_data_price_columns, list):
        market_data_price_columns = list(market_data_price_columns)

    try:
        market_data_actor = PandasQuoteProviderActor.start(portfolio_actor, orderbook_actor, market_data, market_data_price_columns)

        order_futures = []
        LOG.info("Place Orders ...")
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

                    # finally we can send of the order to the orderbook actor
                    order = order_type(asset, **{"size": None, **order_args, "valid_from": valid_from})
                    LOG.debug(f"place order: {order}")
                    order_futures.append(orderbook_actor.ask(NewOrderMessage(order), block=False))

        # before we send of market data and trigger the whole system to simulate order execution and portfolio valuation
        # we wait until all orders are placed
        _ = [f.get() for f in order_futures]

        LOG.debug("full orderbook", orderbook_actor.proxy().get_full_orderbook().get())

        LOG.info("Replay Market Data")
        used_marketdata_frame = market_data_actor.ask(ReplayAllMarketDataMessage())

        LOG.info("Ask for executed orders")
        executed_orders_frame = orderbook_actor.ask(AllExecutedOrderHistory())

        LOG.info("Ask for strategy performance")
        portfolio_result_frames = portfolio_actor.ask(PortfolioPerformanceMessage(resample_rule=resample_rule))

        return used_marketdata_frame, executed_orders_frame, *portfolio_result_frames
    finally:
        if shutdown_on_complete:
            LOG.info(f"shutting down actors: {portfolio_actor}, {orderbook_actor}, {market_data_actor}")
            pykka.ActorRegistry.stop_all()

