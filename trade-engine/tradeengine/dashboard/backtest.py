import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
from tradeengine.plot.plot_backtest import PlotBacktest


def backtest_layout(app, plot_bt: PlotBacktest):
    fig_positions = make_subplots(rows=2, cols=1, vertical_spacing=0.03)

    layout = html.Div(
        [
            dbc.Row(
                dbc.Col(html.H2("Backtest", className='text-center'), width="12")
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(id='figure_timeseries', figure=
                            plot_bt.plot_performance()\
                                .update_layout(height=1000, barmode='relative', bargap=0, bargroupgap=0)\
                                .update(layout_xaxis_rangeslider_visible=False)\
                                .update_yaxes(fixedrange=True)
                        ),
                        width=9
                    ),
                    dbc.Col(
                        dcc.Graph(id='figure_positions', figure=fig_positions.update_layout(height=1000)),
                        width=3
                    ),
                ],
            ),
            dbc.Row(
                dbc.Col(
                    plot_bt.get_orders_table(),
                    width=12,
                )
            ),
        ],
    )

    # Callback function to update figure2 on click
    @app.callback(Output('figure_positions', 'figure'), Input('figure_timeseries', 'clickData'))
    def update_figure2(click_data):
        x_value = click_data['points'][0]['x'] if click_data is not None else None
        return plot_bt.plot_positions(x_value)

    return layout


