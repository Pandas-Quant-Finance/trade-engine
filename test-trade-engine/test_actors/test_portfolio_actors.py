from datetime import datetime

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from unittest import TestCase
from numpy import testing as nt

from tradeengine.actors.sql.persitency import PortfolioBase, PortfolioTrade
from tradeengine.actors.sql.portfolio import SQLPortfolioActor, CASH
from tradeengine.dto.dataflow import Asset

AAPL = Asset("AAPL")
MSFT = Asset("MSFT")


# TODO google how to run the same test for various sub-classes
# TODO eventually implement a memory version of the sql portfolio actor (should be copy paste + abstract data access)
#@ddt
class TestPortfolioActor(TestCase):

    def _test_add_position_dev(self):
        engine = create_engine('sqlite:///foo.db', echo=True)

        port = SQLPortfolioActor(engine)
        port.add_new_position(AAPL, datetime.now(), 10, 12.2, 0)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)

        port.add_new_position(AAPL, datetime.now(), -5, 13.2, 0)
        port.add_new_position(AAPL, datetime.now(), -5, 11.2, 0)
        print(port.get_portfolio_value(datetime.now()))

    # TODO use ddt and data to test every implementaion of the portfolio actor
    #@data(SQLPortfolioActor(create_engine('sqlite://', echo=True)),)
    #def test_add_position(self, port):
    def test_add_position(self):
        port = SQLPortfolioActor(create_engine('sqlite://', echo=True))

        port.add_new_position(AAPL, datetime.now(), 10, 12.2, 0)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)
        self.assertEquals(values.positions[AAPL].value, 122.0)
        self.assertEquals(values.positions[AAPL].qty, 10)
        self.assertEquals(values.positions[CASH].value, -121.0)

        port.add_new_position(AAPL, datetime.now(), -5, 13.2, 0)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)
        self.assertEquals(values.positions[AAPL].value, 13.2 * 5)
        self.assertEquals(values.positions[AAPL].qty, 5)
        self.assertEquals(values.positions[CASH].value, -121 + 13.2 * 5)

        port.add_new_position(AAPL, datetime.now(), -6, 11.2, 0)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)
        self.assertEquals(values.positions[AAPL].value, -11.2)
        self.assertEquals(values.positions[AAPL].qty, -1)
        self.assertEquals(values.positions[CASH].value, -121 + 13.2 * 5 + 11.2 * 6)

        port.add_new_position(AAPL, datetime.now(), 1, 10.2, 0)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)
        self.assertEquals(values.positions[AAPL].value, 0)
        self.assertEquals(values.positions[AAPL].qty, 0)
        self.assertEquals(values.positions[CASH].value, -121 + 13.2 * 5 + 11.2 * 6 - 10.2)

        # finalize
        port.on_stop()

    def test_evaluate_position(self):
        port = SQLPortfolioActor(create_engine('sqlite://', echo=True))

        port.add_new_position(AAPL, datetime.now(), 10, 12.2, 0)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)
        self.assertEquals(values.positions[AAPL].value, 122.0)
        self.assertEquals(values.positions[CASH].value, -121.0)

        port.update_position_value(AAPL, datetime.now(), 13.2, 13.2)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)
        self.assertEquals(values.positions[AAPL].value, 132.0)
        self.assertEquals(values.positions[CASH].value, -121.0)

        port.update_position_value(AAPL, datetime.now(), 11.2, 11.2)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)
        self.assertEquals(values.positions[AAPL].value, 112.0)
        self.assertEquals(values.positions[CASH].value, -121.0)

        port.add_new_position(AAPL, datetime.now(), -5, 12.2, 0)
        values = port.get_portfolio_value(datetime.now())
        self.assertEquals(len(values.positions), 2)
        self.assertEquals(values.positions[AAPL].value, 122.0 / 2.)
        self.assertEquals(values.positions[CASH].value, -121.0 + 12.2 * 5)

        # check timeseries
        df = port.get_portfolio_timeseries().set_index(["asset", "time"])
        self.assertEquals(df.loc[CASH, "value"].to_list(), [1, -121, -60])
        self.assertEquals(df.loc[AAPL, "value"].to_list(), [122, 132, 112, 12.2 * 5])

        hist = port.get_performance_history()
        # print(hist)

        nt.assert_array_almost_equal(
            hist[0].values,
            np.array([
                [1.0, np.nan],
                [-121, 122],
                [-121, 132],
                [-121, 112],
                [-60, 61],
            ])
        )
        nt.assert_array_almost_equal(
            hist[1]['performance'].values,
            np.array([1, 1, 11, -9, 1])
        )

        self.assertEquals((hist[0].index[1] - hist[0].index[0]).days, 1)
        self.assertEquals((hist[1].index[1] - hist[1].index[0]).days, 1)

        # check trades
        trades = port.get_trades().set_index(['asset', 'time'])
        nt.assert_array_almost_equal(
            trades.loc[CASH, ['quantity', 'cost']].values,
            np.array([
                [1, 1],
                [-122, 1],
                [61, 1],
            ])
        )
        nt.assert_array_almost_equal(
            trades.loc[AAPL, ['quantity', 'cost']].values,
            np.array([
                [10, 12.2],
                [-5, 12.2],
            ])
        )

        # finalize
        port.on_stop()

    def test_weights(self):
        port = SQLPortfolioActor(create_engine('sqlite://', echo=True), funding=301)

        port.add_new_position(AAPL, datetime.now(), 10, 20, 0)
        port.add_new_position(MSFT, datetime.now(), 10, 10, 0)

        pv = port.get_portfolio_value()
        self.assertEquals(pv.cash, 1)
        nt.assert_array_almost_equal(
            np.array([p.weight for p in pv.positions.values()]),
            np.array([0.003322, 0.664452, 0.332226]),
        )

        port.update_position_value(MSFT, datetime.now(), 20, 20)
        port.update_position_value(AAPL, datetime.now(), 10, 10)

        pv = port.get_portfolio_value()
        self.assertEquals(pv.cash, 1)
        nt.assert_array_almost_equal(
            np.array([p.weight for p in pv.positions.values()]),
            np.array([0.003322, 0.332226, 0.664452]),
        )

        # finalize
        port.on_stop()

        # restart use leverage weights
        port = SQLPortfolioActor(create_engine('sqlite://', echo=True), funding=1)

        port.add_new_position(AAPL, datetime.now(), 10, 20, 0)
        port.add_new_position(MSFT, datetime.now(), 10, 10, 0)

        pv = port.get_portfolio_value()
        self.assertEquals(pv.cash, -299)
        nt.assert_array_almost_equal(
            np.array([p.weight for p in pv.positions.values()]),
            np.array([-299, 200, 100]),
        )

        # finalize
        port.on_stop()

    def test_proceed_with_portfolio(self):
        # TODO ...
        pass
