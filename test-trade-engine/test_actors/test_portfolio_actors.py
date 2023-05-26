from datetime import datetime

import numpy as np
import pytest
from numpy import testing as nt
from sqlalchemy import create_engine

from testutils.data import AAPL, MSFT
from testutils.database import get_sqlite_engine
from tradeengine.actors.memory import MemPortfolioActor
from tradeengine.actors.sql.sql_portfolio import SQLPortfolioActor, CASH


@pytest.mark.parametrize(
    "actor",
    [
        lambda f: SQLPortfolioActor(get_sqlite_engine(True), funding=f),
        lambda f: MemPortfolioActor(funding=f),
    ]
)
class TestPortfolioActor:

    def test_add_position(self, actor):
        port = actor(1)

        port.add_new_position(AAPL, datetime.now(), 10, 12.2, 0)
        values = port.get_portfolio_value(None)
        assert len(values.positions) == 2
        assert values.positions[AAPL].value == 122.0
        assert values.positions[AAPL].qty == 10
        assert values.positions[CASH].value == -121.0

        port.add_new_position(AAPL, datetime.now(), -5, 13.2, 0)
        values = port.get_portfolio_value(None)
        assert len(values.positions) == 2
        assert values.positions[AAPL].value == 13.2 * 5
        assert values.positions[AAPL].qty == 5
        assert values.positions[CASH].value == -121 + 13.2 * 5

        port.add_new_position(AAPL, datetime.now(), -6, 11.2, 0)
        values = port.get_portfolio_value(None)
        assert len(values.positions) == 2
        assert values.positions[AAPL].value == -11.2
        assert values.positions[AAPL].qty == -1
        assert values.positions[CASH].value == -121 + 13.2 * 5 + 11.2 * 6

        port.add_new_position(AAPL, datetime.now(), 1, 10.2, 0)
        values = port.get_portfolio_value(None)
        assert len(values.positions) == 2
        assert values.positions[AAPL].value == 0
        assert values.positions[AAPL].qty == 0
        assert values.positions[CASH].value == -121 + 13.2 * 5 + 11.2 * 6 - 10.2

        # finalize
        port.on_stop()

    def test_evaluate_position(self, actor):
        port = actor(1)

        port.add_new_position(AAPL, datetime.now(), 10, 12.2, 0)
        values = port.get_portfolio_value(None)
        assert len(values.positions) == 2
        assert values.positions[AAPL].value == 122.0
        assert values.positions[CASH].value == -121.0

        port.update_position_value(AAPL, datetime.now(), 13.2, 13.2)
        values = port.get_portfolio_value(None)
        assert len(values.positions) == 2
        assert values.positions[AAPL].value == 132.0
        assert values.positions[CASH].value == -121.0

        port.update_position_value(AAPL, datetime.now(), 11.2, 11.2)
        values = port.get_portfolio_value(None)
        assert len(values.positions) == 2
        assert values.positions[AAPL].value == 112.0
        assert values.positions[CASH].value == -121.0

        port.add_new_position(AAPL, datetime.now(), -5, 12.2, 0)
        values = port.get_portfolio_value(None)
        assert len(values.positions) == 2
        assert values.positions[AAPL].value == 122.0 / 2.
        assert values.positions[CASH].value == -121.0 + 12.2 * 5

        # check timeseries
        df = port.get_portfolio_timeseries().set_index(["asset", "time"])
        assert df.loc[CASH, "value"].to_list() == [1, -121, -60]
        assert df.loc[AAPL, "value"].to_list() == [122, 132, 112, 12.2 * 5]

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

        assert (hist[0].index[1] - hist[0].index[0]).days == 1
        assert (hist[1].index[1] - hist[1].index[0]).days == 1

        # finalize
        port.on_stop()

    def test_weights(self, actor):
        port = actor(301)

        port.add_new_position(AAPL, datetime.now(), 10, 20, 0)
        port.add_new_position(MSFT, datetime.now(), 10, 10, 0)

        pv = port.get_portfolio_value()
        assert pv.cash == 1
        nt.assert_array_almost_equal(
            np.array([p.weight for p in pv.positions.values()]),
            np.array([0.003322, 0.664452, 0.332226]),
        )

        port.update_position_value(MSFT, datetime.now(), 20, 20)
        port.update_position_value(AAPL, datetime.now(), 10, 10)

        pv = port.get_portfolio_value()
        assert pv.cash == 1
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
        assert pv.cash == -299
        nt.assert_array_almost_equal(
            np.array([p.weight for p in pv.positions.values()]),
            np.array([-299, 200, 100]),
        )

        # finalize
        port.on_stop()

    def test_multiple_trades(self, actor):
        port = actor(1)

        time = datetime.now()
        port.add_new_position(AAPL, time, 10, 20, 0)
        port.add_new_position(MSFT, time, 10, 10, 0)


    def test_proceed_with_portfolio(self, actor):
        port = actor(1)

        # TODO ...
        pass
