import datetime
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List

import pandas as pd
import pykka

from tradeengine.actors.memory import PandasQuoteProviderActor
from tradeengine.dto.dataflow import Asset
from tradeengine.messages import NewOrderMessage, ReplayAllMarketDataMessage, PortfolioPerformanceMessage, \
    AllExecutedOrderHistory

LOG = logging.getLogger(__name__)

@dataclass(frozen=True, eq=True)
class Backtest:
    market_data: pd.DataFrame
    signals: pd.DataFrame
    orders: pd.DataFrame
    position_values: pd.DataFrame
    position_weights:pd.DataFrame
    porfolio_performance: pd.DataFrame
    market_data_extra_data: pd.DataFrame = pd.DataFrame({})

    @property
    def assets(self):
        return set([c[0] for c in self.market_data.columns])

    def save(self, filename):
        self.market_data.to_hdf(filename, key='market_data')
        self.signals.to_hdf(filename, key='signals')
        self.orders.to_hdf(filename, key='orders')
        self.position_values.to_hdf(filename, key='position_values')
        self.position_weights.to_hdf(filename, key='position_weights')
        self.porfolio_performance.to_hdf(filename, key='porfolio_performance')
        self.market_data_extra_data.to_hdf(filename, key='market_data_extra_data')

    @staticmethod
    def load(filename) -> 'Backtest':
        return Backtest(
            pd.read_hdf(filename, key='market_data'),
            pd.read_hdf(filename, key='signals'),
            pd.read_hdf(filename, key='orders'),
            pd.read_hdf(filename, key='position_values'),
            pd.read_hdf(filename, key='position_weights'),
            pd.read_hdf(filename, key='porfolio_performance'),
            pd.read_hdf(filename, key='market_data_extra_data'),
        )


def backtest_strategy(
        orderbook_actor: pykka.ActorRef,
        portfolio_actor: pykka.ActorRef,
        market_data: Dict[Asset, pd.DataFrame],
        signal: Dict[Asset, pd.Series],  # pass a series of [pd.Timestamp, Dict[Type[Order], Dict[str, Any]]]]
        market_data_price_columns: List = ("Open", "High", "Low", "Close"),
        market_data_extra_data: Dict[Asset, pd.DataFrame] = None,
        market_data_interval: timedelta = timedelta(seconds=1),
        resample_rule='D',
        shutdown_on_complete: bool = True,
) -> Backtest:
    if not isinstance(market_data_price_columns, list):
        market_data_price_columns = list(market_data_price_columns)

    market_data_actor = PandasQuoteProviderActor.start(portfolio_actor, orderbook_actor, market_data, market_data_price_columns)
    all_evaluated_signals = {}

    try:
        order_futures = []
        LOG.info("Place Orders ...")
        for asset, signals in signal.items():
            all_evaluated_signals[asset] = {"index": [], "value": []}
            next_timestamps = signals.index.to_series().shift(-1)

            # signal needs to be Dict[Type[Order], Dict[str, Any]]
            for tst, signal in signals.items():
                all_evaluated_signals[asset]["index"].append(tst)
                all_evaluated_signals[asset]["value"].append([])
                next_tst = next_timestamps[tst]

                if signal is None:
                    continue

                if isinstance(tst, pd.Timestamp):
                    tst = tst.to_pydatetime()
                    next_tst = next_tst.to_pydatetime()

                for order_type, order_args in signal.items():
                    # prevent lookahead bias!!
                    valid_from = tst + market_data_interval

                    # convert any dates to pydates
                    for oa, val in order_args.items():
                        if isinstance(val, pd.Timestamp):
                            order_args[oa] = val.to_pydatetime()

                    # finally we can send of the order to the orderbook actor
                    order = order_type(asset, **{"size": None, "valid_until": next_tst if not pd.isna(next_tst) else datetime.datetime.max, **order_args, "valid_from": valid_from})
                    LOG.debug(f"place order: {order}")
                    order_futures.append(orderbook_actor.ask(NewOrderMessage(order), block=False))
                    all_evaluated_signals[asset]["value"][-1].append(order)

        # before we send of market data and trigger the whole system to simulate order execution and portfolio valuation
        # we wait until all orders are placed
        _ = [f.get() for f in order_futures]

        LOG.debug("full orderbook", orderbook_actor.proxy().get_full_orderbook().get())

        LOG.info("Replay Market Data")
        used_marketdata_frame = market_data_actor.ask(ReplayAllMarketDataMessage())

        LOG.info("Ask for executed orders")
        executed_orders_frame = orderbook_actor.ask(AllExecutedOrderHistory(include_evicted=True))

        LOG.info("Ask for strategy performance")
        portfolio_result_frames = portfolio_actor.ask(PortfolioPerformanceMessage(resample_rule=resample_rule))

        # add extra info to market data
        if market_data_extra_data is not None:
            market_data_extra_data = pd.concat(market_data_extra_data.values(), keys=market_data_extra_data.keys(), axis=1, sort=True)
        else:
            market_data_extra_data = pd.DataFrame({})

        # make signals dataframe
        trading_signals = pd.concat([pd.Series(s["value"], index=s["index"]) for s in all_evaluated_signals.values()], keys=all_evaluated_signals.keys(), axis=1, sort=True)
        return Backtest(used_marketdata_frame, trading_signals, executed_orders_frame, *portfolio_result_frames, market_data_extra_data)
    finally:
        if shutdown_on_complete:
            LOG.info(f"shutting down actors: {portfolio_actor}, {orderbook_actor}, {market_data_actor}")
            pykka.ActorRegistry.stop_all()
