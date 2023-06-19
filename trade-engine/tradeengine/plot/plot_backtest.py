from collections import defaultdict
from typing import Dict, Literal

import dash.dash_table as ddt
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from tradeengine.backtest import Backtest
from tradeengine.dto.asset import CASH
from tradeengine.plot.colors import get_color_for


# FIXME
#  use Asset object in orderbook query
#  add link between order in orderbook and signal-order
#  plot signal orders with line to executed orders


class PlotBacktest(object):

    def __init__(self, backtest: Backtest) -> None:
        super().__init__()
        self.backtest = backtest

    def plot_performance(self):
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.6, 0.2, 0.2], subplot_titles=("Performance", "Position Changes", "Position Values"))
        traces = self.get_plot_objects()

        fig.add_traces(traces["portfolio_performance"], rows=1, cols=1)
        fig.add_traces(traces["market_data"], rows=1, cols=1)
        fig.add_traces(traces["signal"], rows=1, cols=1)
        fig.add_traces(traces["executed_orders"], rows=2, cols=1)
        fig.add_traces(traces["position_values"], rows=3, cols=1)

        return fig

    def plot_positions(self, tst=None):
        specs = [[{"type": "pie"}], [{"type": "pie"}]]
        fig_positions = make_subplots(
            rows=2, cols=1, vertical_spacing=0.03, subplot_titles=("Position Weights", "Position Values"), specs=specs)

        x_value = tst if tst is not None else self.backtest.position_values.index[-1]
        print("x", x_value)
        fig_positions.add_trace(
            go.Pie(
                labels=list(map(str, self.backtest.position_weights.loc[x_value].index)),
                values=self.backtest.position_weights.loc[x_value].abs().apply(lambda x: f"{x:.4f}"),
                marker_colors=list(map(get_color_for, self.backtest.position_weights.loc[x_value].index)),
                sort=False,
            ),
            row=1,
            col=1
        )

        fig_positions.add_trace(
            go.Pie(
                labels=list(map(str, self.backtest.position_values.loc[x_value].index)),
                values=self.backtest.position_values.loc[x_value].abs().apply(lambda x: f"{x:.4f}"),
                marker_colors=list(map(get_color_for, self.backtest.position_values.loc[x_value].index)),
                sort=False,
            ),
            row=2, col=1
        )

        return fig_positions

    def get_plot_objects(self) -> Dict[str, list]:
        orders = self.backtest.orders[self.backtest.orders["status"] == 1]\
            .pivot(index='execute_time', columns='asset', values='execute_value')\
            .sort_index()
        idx = self.backtest.market_data.index

        # store traces in dict
        traces = defaultdict(list)

        # performance
        traces["portfolio_performance"] = [go.Scatter(x=self.backtest.porfolio_performance.index, y=self.backtest.porfolio_performance["performance"], mode='lines', name="Portfolio", legendgroup="Portfolio", marker=dict(color='#555555'))]

        # cash position
        #traces["position_values"].append(go.Scatter(x=self.position_values.index, y=self.position_values[CASH], mode='lines', name="Cash", legendgroup="Cash", marker=dict(color='#555555')))
        traces["position_values"].append(go.Bar(x=self.backtest.position_values.index, y=self.backtest.position_values[str(CASH)], name="Cash", legendgroup="Cash", marker=dict(color='#555555')))

        # Add traces to the first row
        visible = 'legendonly' if len(self.backtest.assets) > 3 else True
        for asset in self.backtest.assets:
            symbol = str(asset)
            color = get_color_for(asset)

            # plot market data
            md = self.backtest.market_data[asset]
            scale_factor = md.loc[md.first_valid_index()].mean()
            md /= scale_factor
            cols = md.columns.tolist()
            ncols = md.shape[1]
            ohlc = cols[:4] if ncols >= 4 else (cols[0] * 2 + cols[1] * 2 if ncols == 2 else cols[0] * 4)
            trace_price = go.Ohlc(x=idx, open=md[ohlc[0]], high=md[ohlc[1]], low=md[ohlc[2]], close=md[ohlc[3]], name=symbol, legendgroup=symbol, increasing_line_color=color, decreasing_line_color=color, visible=visible)
            traces["market_data"].append(trace_price)

            if len(self.backtest.market_data_extra_data) > 0:
                ext_data = self.backtest.market_data_extra_data[asset] / scale_factor
                for ext_col in ext_data.columns:
                    trace_price = go.Scatter(x=ext_data.index, y=ext_data[ext_col], name=f"{symbol}.{ext_col}", legendgroup=symbol, mode='lines', visible=visible)
                    traces["market_data"].append(trace_price)

            # plot raw signals
            signals = self.backtest.signals[asset]  # Dict[Class[<Order], params]
            markers = defaultdict(lambda: {"x": [], "y": []})
            for i, sigs in signals.items():
                for s in sigs:
                    if s is None: continue
                    markers[s["marker"]]["x"].append(i)
                    markers[s["marker"]]["y"].append(md.loc[i].mean())

            for marker, xy in markers.items():
                trace_signal = go.Scatter(x=xy["x"], y=xy["y"], name=symbol, marker=dict(color=color, symbol=marker, size=10), legendgroup=symbol, showlegend=False, mode='markers', visible=visible)
                traces["signal"].append(trace_signal)

            # plot executed orders
            if symbol in orders.columns:
                trace_executed_order = go.Bar(x=orders.index, y=orders[symbol].values, marker=dict(color=color), name=symbol, legendgroup=symbol, showlegend=False, visible=visible)
                traces["executed_orders"].append(trace_executed_order)

            # position values
            if symbol in self.backtest.position_values.columns:
                pv = self.backtest.position_values[asset]
                position_value = go.Bar(x=pv.index, y=pv, marker=dict(color=color), name=symbol, legendgroup=symbol, showlegend=False, visible=visible)
                traces["position_values"].append(position_value)

        # return all traces:
        return traces

    def get_plot_trade_details_objects(self, x, date_filter: Literal['valid_from', 'execute_time'] = 'valid_from'):
        import plotly.graph_objects as go

        orders_from_x = self.backtest.orders[(self.backtest.orders[date_filter] == x).values]

        buy_orders_from_x = orders_from_x[orders_from_x["qty"] >= 0]
        buy_orders_from_x_trace = go.Pie(labels=buy_orders_from_x["symbol"].values, values=buy_orders_from_x["qty"].values)

        sell_orders_from_x = orders_from_x[orders_from_x["qty"] < 0]
        sell_orders_from_x_trace = go.Pie(labels=sell_orders_from_x["symbol"].values, values=sell_orders_from_x["qty"].values)

        pass

    def get_orders_table(self):
        orders = self.backtest.orders.copy()
        orders["order_type"] = orders["order_type"].astype(str)
        return ddt.DataTable(orders.to_dict('records'), id='orders-table')
