import datetime
import logging
import click
import sys
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from typing import Dict, List, Hashable, Tuple, Any

import pandas as pd
import pykka
from pykka._future import Future

import tradeengine
from tradeengine.actors.memory import PandasQuoteProviderActor
from tradeengine.dto import Asset, Order
from tradeengine.messages import NewOrderMessage, ReplayAllMarketDataMessage, PortfolioPerformanceMessage, \
    AllExecutedOrderHistory

LOG = logging.getLogger(__name__)
ORDER_MODULE = tradeengine.dto.order.__name__


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


class BacktestStrategy(object):

    def __init__(
            self,
            orderbook_actor: pykka.ActorRef,
            portfolio_actor: pykka.ActorRef,
            market_data: Dict[Hashable, pd.DataFrame],
            market_data_price_columns: List = ("Open", "High", "Low", "Close"),
            market_data_extra_data: Dict[Hashable, pd.DataFrame] = None,
            market_data_interval: timedelta = timedelta(seconds=1),
    ):
        self.orderbook_actor = orderbook_actor
        self.portfolio_actor = portfolio_actor
        self.market_data = market_data
        self.market_data_price_columns = list(market_data_price_columns) if not isinstance(market_data_price_columns, list) else market_data_price_columns
        self.market_data_extra_data = market_data_extra_data if market_data_extra_data is not None else {k: pd.DataFrame({}) for k in market_data.keys()}
        self.market_data_interval = market_data_interval

    def run_backtest(
            self,
            signals: Dict[Hashable, pd.Series],  # pass a series of [pd.Timestamp, Dict[str[Type[<Order]], kwargs]]]
            resample_rule: str = 'D',
            shutdown_on_complete: bool = True
    ) -> Backtest:
        market_data = self.market_data

        # create orders from signals
        orders = {
            h: pd.Series(map(self._gen_trading_days_aware_orders(h), s.items()), index=s.index)\
                for h, s in signals.items()
        }

        # ask orderbook about all orders we want to place
        #  NOTE in order to return this data structure we need to json serialize the Order/Asset objects
        placed_orders = {
            a: s.apply(self._place_order).apply(lambda futures: [f.get().todict() for f in futures if f.get() is not None])\
                for a, s in orders.items()
        }

        # generate market data for market data actor
        market_data = {Asset(h): df for h, df in market_data.items()}
        market_data_actor = PandasQuoteProviderActor.start(
            self.portfolio_actor, self.orderbook_actor, market_data, self.market_data_price_columns
        )

        try:
            LOG.debug("full orderbook", self.orderbook_actor.proxy().get_full_orderbook().get())

            LOG.info("Replay Market Data")
            used_marketdata_frame = market_data_actor.ask(ReplayAllMarketDataMessage())

            LOG.info("Ask for executed orders")
            executed_orders_frame = self.orderbook_actor.ask(AllExecutedOrderHistory(include_evicted=True))

            LOG.info("Ask for strategy performance")
            portfolio_result_frames = self.portfolio_actor.ask(PortfolioPerformanceMessage(resample_rule=resample_rule))

            # add extra info to market data
            market_data_extra_data = \
                pd.concat(self.market_data_extra_data.values(), keys=self.market_data_extra_data.keys(), axis=1, sort=True)

            # make signals dataframe
            trading_signals = pd.concat(placed_orders.values(), keys=placed_orders.keys(), axis=1, sort=True)

            # return all frame results
            return Backtest(
                used_marketdata_frame, trading_signals, executed_orders_frame, *portfolio_result_frames, market_data_extra_data
            )
        finally:
            if shutdown_on_complete:
                LOG.info(f"shutting down actors: {self.portfolio_actor}, {self.orderbook_actor}, {market_data_actor}")
                pykka.ActorRegistry.stop_all()
            else:
                try:
                    market_data_actor.stop(True)
                except Exception as ignore:
                    LOG.error("ignored error: ", ignore)

    def _gen_trading_days_aware_orders(self, symbol: Hashable):
        market_data_index: pd.DatetimeIndex = self.market_data[symbol].index
        trading_days_4_asset = market_data_index.to_series().shift(-1).fillna(datetime.datetime.max)
        return partial(self._make_orders, market_data_index=trading_days_4_asset, asset=Asset(symbol))

    def _make_orders(self, order_descriptions: Tuple[pd.Timestamp, Dict[str, Dict[str, Any]]], market_data_index: pd.Series, asset: Asset):
        tst, order_description = order_descriptions
        if order_description is None: return []

        next_trading_tst = market_data_index.loc[tst]

        if isinstance(tst, pd.Timestamp): tst = tst.to_pydatetime()
        if isinstance(next_trading_tst, pd.Timestamp): next_trading_tst = next_trading_tst.to_pydatetime()

        # prevent lookahead bias!!
        valid_from = tst + self.market_data_interval

        def make_order(order_type__kwargs):
            order_type, order_kwargs = order_type__kwargs
            if isinstance(order_type, str): order_type = getattr(sys.modules[ORDER_MODULE], order_type)

            return order_type(
                asset,
                **{
                    "size": None,
                    "valid_until": next_trading_tst,
                    **order_kwargs,
                    "valid_from": valid_from
                }
            )

        orders = list(map(make_order, order_description.items()))
        return orders

    def _place_order(self, orders: List[Order]) -> List[Future]:
        return [self.orderbook_actor.ask(NewOrderMessage(order), block=False) for order in orders]


@click.command()
@click.option('-s', '--signals', type=str, help="glob string of signal csv files")
@click.option('-q', '--quote-frames', type=str, help="glob string of quote csv files")
@click.argument('out_file', nargs=1)
def cli(signals: str, quote_frames: str, out_file: str):
    from pathlib import Path

    signals = {f.name: pd.read_csv(f, parse_dates=True, index_col="Date") for f in Path(".").glob(signals)}
    quote_frames = {f.name: pd.read_csv(f, parse_dates=True, index_col="Date") for f in Path(".").glob(quote_frames)}
    run(signals, quote_frames, out_file)


def run(signals: Dict[Hashable, pd.Series], quote_frames: Dict[Hashable, pd.DataFrame], out_file: str):
    import uuid
    from sqlalchemy import create_engine, StaticPool
    from tradeengine.actors.memory import MemPortfolioActor
    from tradeengine.actors.sql import SQLOrderbookActor

    strategy_id: str = str(uuid.uuid4())
    portfolio_actor = MemPortfolioActor.start(funding=100)
    orderbook_actor = SQLOrderbookActor.start(
        portfolio_actor,
        create_engine('sqlite://', echo=False, connect_args={'check_same_thread': False}, poolclass=StaticPool),
        strategy_id=strategy_id
    )

    backtest = BacktestStrategy(orderbook_actor, portfolio_actor, quote_frames).run_backtest(signals)

    if out_file is not None:
        backtest.save(out_file)


if __name__ == '__main__':
    cli()
