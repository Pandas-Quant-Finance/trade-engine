from abc import abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, List, Tuple

import pykka

from tradeengine.dto.dataflow import PositionValue, OrderTypes, PortfolioValue, Order, CloseOrder, QuantityOrder, \
    TargetQuantityOrder, PercentOrder, TargetWeightOrder, Asset
from tradeengine.messages.messages import QuantityOrderMessage, PercentOrderMessage, \
    NewBidAskMarketData, NewBarMarketData, CloseOrderMessage, TargetQuantityOrderMessage, TargetWeightOrderMessage, \
    PortfolioValueMessage, NewPositionMessage

RELATIVE_ORDER_TYPES = (OrderTypes.TARGET_QUANTITY, OrderTypes.PERCENT, OrderTypes.TARGET_WEIGHT)

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


class AbstractOrderbookActor(pykka.ThreadingActor):

    def __init__(
            self,
            portfolio_actor: pykka.ActorRef,
    ):
        super().__init__()
        self.portfolio_actor = portfolio_actor

    def on_receive(self, message: Any) -> Any:
        match message:
            # if message is PlaceOrder, we store the order in the orderbook
            case QuantityOrderMessage(asset, limit, stop_limit, valid_from, valid_until, qty):
                return self.place_order(OrderTypes.QUANTITY, asset, qty, valid_from, limit, stop_limit, valid_until)
            case TargetQuantityOrderMessage(asset, limit, stop_limit, valid_from, valid_until, qty):
                return self.place_order(OrderTypes.TARGET_QUANTITY, asset, qty, valid_from, limit, stop_limit, valid_until)
            case PercentOrderMessage(asset, limit, stop_limit, valid_from, valid_until, percent):
                return self.place_order(OrderTypes.PERCENT, asset, percent, valid_from, limit, stop_limit, valid_until)
            case TargetWeightOrderMessage(asset, limit, stop_limit, valid_from, valid_until, weight):
                return self.place_order(OrderTypes.TARGET_WEIGHT, asset, weight, valid_from, limit, stop_limit, valid_until)
            case CloseOrderMessage(asset, limit, stop_limit, valid_from, valid_until):
                return self.place_order(OrderTypes.CLOSE, asset, None, valid_from, limit, stop_limit, valid_until)

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
        # evict orders and return if nothing to execute
        self._evict_orders(asset, as_of)

        # check if we have an order and if an order would be executed (or could be evicted)
        execute = self._get_orders_for_execution(asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask)
        if len(execute) <= 0: return

        # check if we have orders which need the portfolio value to be executable. And sort such that we sell first
        # before we increase positions
        need_portfolio_value = any([True for o, _ in execute if o.type in RELATIVE_ORDER_TYPES])
        pv: PortfolioValue = self.portfolio_actor.ask(PortfolioValueMessage()) if need_portfolio_value else None
        execute_order = sum([order.to_quantity(pv, price) for order, price in execute])
        avg_price = sum([order.size * price for order, price in execute]) / sum([order.size for order, _ in execute])

        # we only have one net order for one asset from one asset's price update which we execute now.
        # and then tell the portfolio actor about it
        quantity, price, fee = self._execute_order(execute_order, avg_price, pv)
        if quantity is not None:
            self.portfolio_actor.tell(NewPositionMessage(asset, as_of, quantity, price, fee))

    @abstractmethod
    def place_order(
            self,
            order_type: OrderTypes,
            asset: Asset,
            qty: float | None,
            valid_from: datetime,
            limit: float | None = None,
            stop_limit: float | None = None,
            valid_until: datetime = None,
    ):
        # simply store the order in a datastructure
        raise NotImplemented

    @abstractmethod
    def _get_orders_for_execution(self, asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask) -> List[Tuple[Order, float]]:
        # all orders where valid_from >= as_of and valid_until >= as_of and where the limit is matched
        raise NotImplemented

    @abstractmethod
    def _execute_order(self, order: QuantityOrder, expected_price: float, pv: PortfolioValue | None) -> Tuple[float | None, float | None, float | None]:
        # delete fully filled orders from the orderbook and put it to the orderbook_history
        # return the definitive traded quantity, price and fee, in case we only want to execute orders which a
        # strong enough portfolio impact (>= x% of portfolio value) we can abort the execution and return None values.
        raise NotImplemented

    @abstractmethod
    def _evict_orders(self, asset: Asset, as_of: datetime):
        # delete orders where valid_until < as_of from the orderbook and put it to the orderbook_history
        raise NotImplemented

