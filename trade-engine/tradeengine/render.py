import click

from tradeengine.backtest import Backtest
from tradeengine.dashboard.backtest import backtest_layout


@click.command()
@click.option('-p', '--port', default=8050, help="port dash server listens to (default 8050)")
@click.argument('filename', nargs=1, required=False, default=None)
def cli(filename, port):
    run(filename, port)


def run(filename: str, port: int | None = None):
    import dash
    import dash_bootstrap_components as dbc
    from tradeengine.plot.plot_backtest import PlotBacktest

    if filename is None:
        # filename = '/home/kic/sources/mine/tradeengine/notebooks/strategy-long-aapl.hdf5'
        filename = '/home/kic/sources/mine/tradeengine/notebooks/strategy-swing-aapl.hdf5'
        # filename = '/home/kic/sources/mine/tradeengine/notebooks/strategy-swing-all.hdf5'
        # filename = '/home/kic/sources/mine/tradeengine/notebooks/strategy-long-1oN.hdf5'

    backtest = Backtest.load(filename)
    plot_bt = PlotBacktest(backtest)

    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    app.layout = backtest_layout(app, plot_bt)
    app.run_server(debug=True, port=8050 if port is None else port)


if __name__ == '__main__':
    cli()
