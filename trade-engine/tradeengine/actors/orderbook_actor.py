import logging
from abc import abstractmethod
from datetime import datetime
from functools import partial
from typing import Any, List, Tuple

import numpy as np
import pandas as pd
import pykka

from tradeengine.dto.portfolio import PortfolioValue
from tradeengine.dto.order import Order, ExpectedExecutionPrice
from tradeengine.dto import Asset, OrderTypes, QuantityOrder
from tradeengine.messages.messages import NewBidAskMarketData, NewBarMarketData, PortfolioValueMessage, \
    NewPositionMessage, NewOrderMessage, AllExecutedOrderHistory

RELATIVE_ORDER_TYPES = (OrderTypes.TARGET_QUANTITY, OrderTypes.PERCENT, OrderTypes.TARGET_WEIGHT, OrderTypes.CLOSE)
LOG = logging.getLogger(__name__)


class AbstractOrderbookActor(pykka.ThreadingActor):
    """
    The OrderbookActor is responsible to keep track of orders the client places.
    It needs a Portfolio Actor to tell in case an order is executed.

    The Actor accepts the following messages:
     * a message to place an order in percentages (weights) or quantity (nr of shares)
     * a message which tells the orderbook about new market quote updates

    The actor sends the following messages:
     * asks the Portfolio Actor about the current total portfolio value
     * tells the Portfolio Actor about new executed trades
    """

    def __init__(
            self,
            portfolio_actor: pykka.ActorRef,
    ):
        super().__init__()
        self.portfolio_actor = portfolio_actor

    def on_stop(self) -> None:
        LOG.debug(f"stopped orderbook actor {self}")

    def on_receive(self, message: Any) -> Any:
        match message:
            # if message is PlaceOrder, we store the order in the orderbook
            case NewOrderMessage(order):
                return self.place_order(order)
            case AllExecutedOrderHistory(include_evicted):
                return self.get_all_executed_orders(include_evicted)

            # when a new quote messages comes in whe need to check if an order is executed or can be evicted.
            # if an order can be executed and the quantity is not clear (weight/percentage/amount orders)
            # we need to ask the portfolio first what the current portfolio value is
            case NewBidAskMarketData(asset, as_of, bid, ask):
                return self.new_market_data(asset, as_of, bid, ask, bid, ask, bid, ask)
            case NewBarMarketData(asset, as_of, open, high, low, close):
                return self.new_market_data(asset, as_of, open, open, high, low, close, close)
            case _:
                raise ValueError(f"Unknown Message {message}")

    def new_market_data(self, asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask):
        # evict orders
        evicted = self._evict_orders(asset, as_of)
        LOG.info(f"number of evicted orders for {asset} @ {as_of}", evicted)

        # check if we have an order and if an order would be executed and return if nothing to execute
        expected_price = ExpectedExecutionPrice(as_of, open_bid, open_ask, close_bid, close_ask)
        executable_orders = self._get_orders_for_execution(asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask)
        LOG.info(f"number of executable orders for {asset} @ {as_of}", len(executable_orders))
        if len(executable_orders) <= 0: return 0

        need_portfolio_value = any(o.type for o in executable_orders if o.type in RELATIVE_ORDER_TYPES) and len(executable_orders) > 1
        pv: PortfolioValue = self.portfolio_actor.ask(PortfolioValueMessage()) if need_portfolio_value else None

        # sort orders by sell orders first:
        definite_executed_orders = 0
        sort_key_function = partial(order_sorter, pv=pv, expected_price=expected_price) if len(executable_orders) > 1 else lambda _:0
        for executable_order in sorted(executable_orders, key=sort_key_function):
            definite_executed_orders += self._execute_executable_order(executable_order, expected_price, asset, as_of)

        # number of orders processed
        LOG.info(f"number of executed orders for {asset} @ {as_of}", definite_executed_orders)
        return definite_executed_orders

    def _execute_executable_order(self, order: Order, expected_price: ExpectedExecutionPrice, asset: Asset, as_of: datetime) -> bool:
        # check if we have orders which need the portfolio value to be executable. And sort such that we sell first
        # before we increase positions
        need_portfolio_value = order.type in RELATIVE_ORDER_TYPES
        pv: PortfolioValue = self.portfolio_actor.ask(PortfolioValueMessage()) if need_portfolio_value else None
        execute_quantity_order = order.to_quantity(pv, expected_price)
        if abs(execute_quantity_order.size) <= 1e-8: return False

        # we only have one net order for one asset from one asset's price update which we execute now.
        # and then tell the portfolio actor about it
        expected_execution_price = expected_price.evaluate_price(np.sign(execute_quantity_order.size), order.valid_from, order.limit)
        quantity, price, fee = self._execute_order(execute_quantity_order, as_of, expected_execution_price, pv)

        if quantity is not None:
            self.portfolio_actor.tell(NewPositionMessage(asset, as_of, quantity, price, fee))
            return True

        return False

    @abstractmethod
    def place_order(self, order: Order) -> Order:
        # simply store the order in a datastructure
        raise NotImplemented

    @abstractmethod
    def _get_orders_for_execution(self, asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask) -> List[Order]:
        # all orders where valid_from >= as_of and valid_until >= as_of and where the limit is matched
        raise NotImplemented

    @abstractmethod
    def _execute_order(self, order: QuantityOrder, expected_execution_time: datetime, expected_price: float, pv: PortfolioValue | None) -> Tuple[float | None, float | None, float | None]:
        # delete fully filled orders from the orderbook and put it to the orderbook_history
        # return the definitive traded quantity, price and fee, in case we only want to execute orders which a
        # strong enough portfolio impact (>= x% of portfolio value) we can abort the execution and return None values.
        raise NotImplemented

    @abstractmethod
    def _evict_orders(self, asset: Asset, as_of: datetime) -> int:
        # delete orders where valid_until < as_of from the orderbook and put it to the orderbook_history
        # returns the number of evicted orders
        raise NotImplemented

    @abstractmethod
    def get_all_executed_orders(self, include_evicted) -> pd.DataFrame:
        raise NotImplemented


def order_sorter(order: Order, expected_price: ExpectedExecutionPrice, pv: PortfolioValue | None):
    """
    we need the following order of orders: fifo by valid_from and then
        CLOSE,
        QUANTITY < 0
        TARGET_QUANTITY - p.qty < 0
        TARGET_WEIGHT - pv.weight < 0
        QUANTITY >= 0
        TARGET_QUANTITY - p.qty >= 0
        TARGET_WEIGHT - pv.weight >= 0
        PERCENT
    """
    match (order.type, order.to_quantity(pv, expected_price).size):
        case (OrderTypes.CLOSE, _):
            return order.valid_from, 0
        case (OrderTypes.QUANTITY, qty) if qty < 0:
            return order.valid_from, 1
        case (OrderTypes.TARGET_QUANTITY, qty) if qty < 0:
            return order.valid_from, 2
        case (OrderTypes.TARGET_WEIGHT, qty) if qty < 0:
            return order.valid_from, 3
        case (OrderTypes.QUANTITY, _):
            return order.valid_from, 4
        case (OrderTypes.TARGET_QUANTITY, _):
            return order.valid_from, 4
        case (OrderTypes.TARGET_WEIGHT, _):
            return order.valid_from, 4
        case (OrderTypes.PERCENT, _):
            return order.valid_from, 5
        case _:
            return order.valid_from, 999
