from datetime import datetime, timedelta
from unittest import TestCase

import numpy as np
from sqlalchemy import create_engine

from tradeengine.actors.sql.orderbook import SQLOrderbookActor
from tradeengine.actors.sql.portfolio import SQLPortfolioActor
from tradeengine.dto.dataflow import Asset, OrderTypes


class TestOrderBookActors(TestCase):

    def test_order_book(self):
        ob = SQLOrderbookActor(None, create_engine('sqlite://', echo=True))
        ob.place_order(OrderTypes.QUANTITY, Asset("AAPL"), 12, datetime.now())

        print(ob.get_full_orderbook())

        print(ob._get_orders_for_execution(Asset("AAPL"), datetime.now(), 2, 2, 2, 2, 2, 2))

        ob._evict_orders(Asset("AAPL"), datetime.now() + timedelta(days=1))
        print(ob.get_full_orderbook())

        ob.on_stop()



    # TODO google how to run the same test for various sub-classes
    def test_something(self):
        port = SQLPortfolioActor(create_engine('sqlite://', echo=True), funding=425)

        port.add_new_position(Asset("AAPL"), datetime.now(), 10, 20, 0)
        port.add_new_position(Asset("MSFT"), datetime.now(), 5, 30, 0)
        port.add_new_position(Asset("TWTR"), datetime.now(), 15, 5, 0)
        print({p.asset: p.weight for p in port.get_portfolio_value().positions.values()})

        port.update_position_value(Asset("AAPL"), datetime.now(), 22, 22)
        port.update_position_value(Asset("MSFT"), datetime.now(), 28, 28)
        port.update_position_value(Asset("TWTR"), datetime.now(), 5, 5)
        weights = {p.asset: p.weight for p in port.get_portfolio_value().positions.values()}
        print(weights)

        target_weights = [0.6, 0.2, 0.2]
        qty = ((0.6 - weights[Asset("AAPL")]) * port.get_portfolio_value().value()) / 22.0

        port.add_new_position(Asset("AAPL"), datetime.now(), qty, 22, 0)
        weights2 = {p.asset: p.weight for p in port.get_portfolio_value().positions.values()}
        print(weights2)

        qty = ((0.2 - weights[Asset("MSFT")]) * port.get_portfolio_value().value()) / 28.0

        port.add_new_position(Asset("MSFT"), datetime.now(), qty, 28, 0)
        weights3 = {p.asset: p.weight for p in port.get_portfolio_value().positions.values()}
        print(weights3)

        qty = ((0.2 - weights[Asset("TWTR")]) * port.get_portfolio_value().value()) / 5.0

        port.add_new_position(Asset("TWTR"), datetime.now(), qty, 5.0, 0)
        weights4 = {p.asset: p.weight for p in port.get_portfolio_value().positions.values()}
        print(weights4)


    def test_something2(self):
        port = SQLPortfolioActor(create_engine('sqlite://', echo=True), funding=425)

        port.add_new_position(Asset("AAPL"), datetime.now(), 10, 20, 0)
        port.add_new_position(Asset("MSFT"), datetime.now(), 5, 30, 0)
        port.add_new_position(Asset("TWTR"), datetime.now(), 15, 5, 0)
        print({p.asset: p.weight for p in port.get_portfolio_value().positions.values()})

        port.update_position_value(Asset("AAPL"), datetime.now(), 22, 22)
        port.update_position_value(Asset("MSFT"), datetime.now(), 28, 28)
        port.update_position_value(Asset("TWTR"), datetime.now(), 5, 5)
        weights = {p.asset: p.weight for p in port.get_portfolio_value().positions.values()}
        print(weights)

        target_weights = [0.6, 0.2, 0.2]
        qty = ((0.6 - weights[Asset("AAPL")]) * port.get_portfolio_value().value()) / 22.0

        port.add_new_position(Asset("AAPL"), datetime.now(), qty, 22.2, 0)
        weights2 = {p.asset: p.weight for p in port.get_portfolio_value().positions.values()}
        print(weights2)

        qty = ((0.2 - weights[Asset("MSFT")]) * port.get_portfolio_value().value()) / 28.0

        port.add_new_position(Asset("MSFT"), datetime.now(), qty, 27.98, 0)
        weights3 = {p.asset: p.weight for p in port.get_portfolio_value().positions.values()}
        print(weights3)

        qty = ((0.2 - weights[Asset("TWTR")]) * port.get_portfolio_value().value()) / 5.0

        port.add_new_position(Asset("TWTR"), datetime.now(), qty, 5.3, 0)
        weights4 = {p.asset: p.weight for p in port.get_portfolio_value().positions.values()}
        print(weights4)

"""
[(Asset(symbol='$$$'), 0.0), (Asset(symbol='AAPL'), 0.47058823529411764), (Asset(symbol='MSFT'), 0.35294117647058826), (Asset(symbol='TWTR'), 0.17647058823529413)]
[(Asset(symbol='$$$'), 0.0), (Asset(symbol='AAPL'), 0.5057471264367817), (Asset(symbol='MSFT'), 0.3218390804597701), (Asset(symbol='TWTR'), 0.1724137931034483)]

"""