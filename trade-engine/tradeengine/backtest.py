import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Iterable, Type, Any, Literal

import pandas as pd
import pykka

from tradeengine.actors.memory import PandasQuoteProviderActor
from tradeengine.dto.dataflow import Asset, OrderTypes
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

    @property
    def assets(self):
        return set([c[0] for c in self.market_data.columns])

    def plot(self):
        from plotly.subplots import make_subplots

        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3], subplot_titles=("Performance", "Position Changes"))
        traces = self.get_plot_objects()

        fig.add_traces(traces["portfolio_performance"], rows=1, cols=1)
        fig.add_traces(traces["market_data"], rows=1, cols=1)
        fig.add_traces(traces["signal"], rows=1, cols=1)
        fig.add_traces(traces["executed_orders"], rows=2, cols=1)

        return fig

    def get_plot_objects(self) -> Dict[str, list]:
        import plotly
        import plotly.graph_objects as go

        color_scale = plotly.colors.qualitative.Light24
        orders = self.orders.pivot(index='execute_time', columns='symbol', values='execute_value').sort_index()
        idx = self.market_data.index

        # store traces in dict
        traces = defaultdict(list)

        # performance
        traces["portfolio_performance"] = [go.Scatter(x=self.porfolio_performance.index, y=self.porfolio_performance["performance"], mode='lines', name="Portfolio", legendgroup="Portfolio", marker=dict(color='#555555'))]

        # Add traces to the first row
        for asset in self.assets:
            symbol = str(asset)
            color = color_scale[hash(asset) % len(color_scale)]

            # plot market data
            md = self.market_data[asset]
            md /= md.iloc[0].mean()
            cols = md.columns.tolist()
            ncols = md.shape[1]
            ohlc = cols[:4] if ncols >= 4 else (cols[0] * 2 + cols[1] * 2 if ncols == 2 else cols[0] * 4)
            trace_price = go.Ohlc(x=idx, open=md[ohlc[0]], high=md[ohlc[1]], low=md[ohlc[2]], close=md[ohlc[3]], name=symbol, legendgroup=symbol, increasing_line_color=color, decreasing_line_color=color)
            traces["market_data"].append(trace_price)

            if ncols > 4:
                for ext_col in cols[4:]:
                    trace_price = go.Scatter(x=idx, y=md[ext_col], name=f"{symbol}.{ext_col}", legendgroup=symbol, mode='lines')
                    traces["market_data"].append(trace_price)

            # plot raw signals
            signals = self.signals[asset]  # Dict[Class[<Order], params]
            markers = defaultdict(lambda: {"x": [], "y": []})
            for i, sigs in signals.items():
                for s in sigs:
                    markers[s.marker]["x"].append(i)
                    markers[s.marker]["y"].append(md.loc[i].mean())

            for marker, xy in markers.items():
                trace_signal = go.Scatter(x=xy["x"], y=xy["y"], name=symbol, marker=dict(color=color, symbol=marker, size=10), legendgroup=symbol, showlegend=False, mode='markers')
                traces["signal"].append(trace_signal)

            # plot executed orders
            trace_executed_order = go.Bar(x=orders.index, y=orders[symbol].values, name=symbol, marker=dict(color=color), legendgroup=symbol, showlegend=False)
            traces["executed_orders"].append(trace_executed_order)

        # return all traces:
        return traces

    def get_plot_trade_details_objects(self, x, date_filter: Literal['valid_from', 'execute_time'] = 'valid_from'):
        import plotly.graph_objects as go

        orders_from_x = self.orders[(self.orders[date_filter] == x).values]

        buy_orders_from_x = orders_from_x[orders_from_x["qty"] >= 0]
        buy_orders_from_x_trace = go.Pie(labels=buy_orders_from_x["symbol"].values, values=buy_orders_from_x["qty"].values)

        sell_orders_from_x = orders_from_x[orders_from_x["qty"] < 0]
        sell_orders_from_x_trace = go.Pie(labels=sell_orders_from_x["symbol"].values, values=sell_orders_from_x["qty"].values)

        pass

    def save(self, filename):
        self.market_data.to_hdf(filename, key='market_data')
        self.signals.to_hdf(filename, key='signals')
        self.orders.to_hdf(filename, key='orders')
        self.position_values.to_hdf(filename, key='position_values')
        self.position_weights.to_hdf(filename, key='position_weights')
        self.porfolio_performance.to_hdf(filename, key='porfolio_performance')

    @staticmethod
    def load(filename) -> 'Backtest':
        return Backtest(
            pd.read_hdf(filename, key='market_data'),
            pd.read_hdf(filename, key='signals'),
            pd.read_hdf(filename, key='orders'),
            pd.read_hdf(filename, key='position_values'),
            pd.read_hdf(filename, key='position_weights'),
            pd.read_hdf(filename, key='porfolio_performance'),
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
                    order = order_type(asset, **{"size": None, "valid_until": next_tst, **order_args, "valid_from": valid_from})
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
            used_marketdata_frame = used_marketdata_frame.join(pd.concat(market_data_extra_data.values(), keys=market_data_extra_data.keys(), axis=1, sort=True))

        # make signals dataframe
        trading_signals = pd.concat([pd.Series(s["value"], index=s["index"]) for s in all_evaluated_signals.values()], keys=all_evaluated_signals.keys(), axis=1, sort=True)
        return Backtest(used_marketdata_frame, trading_signals, executed_orders_frame, *portfolio_result_frames)
    finally:
        if shutdown_on_complete:
            LOG.info(f"shutting down actors: {portfolio_actor}, {orderbook_actor}, {market_data_actor}")
            pykka.ActorRegistry.stop_all()

# FIXME
#  use Asset object in orderbook query
#  add pretty html dataframe
#  add/plot position values
#  plot cash position as a matter of leverage
#  plot position weights per date on click
#  add link between order in orderbook and signal-order
#  plot signal orders with line to executed orders
