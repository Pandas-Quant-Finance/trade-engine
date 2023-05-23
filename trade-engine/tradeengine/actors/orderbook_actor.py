from abc import abstractmethod
from enum import Enum
from typing import Any, List

import pykka

from tradeengine.dto.dataflow import PositionValue, OrderTypes, PortfolioValue, Order, CloseOrder, QuantityOrder, \
    TargetQuantityOrder, PercentOrder, TargetWeightOrder
from tradeengine.messages.messages import QuantityOrderMessage, PercentOrderMessage, QuoteAskMarketData, \
    NewBidAskMarketData, NewBarMarketData, CloseOrderMessage, TargetQuantityOrderMessage, TargetWeightOrderMessage, \
    PortfolioValueMessage

RELATIVE_ORDER_TYPES = (OrderTypes.TARGET_QUANTITY, OrderTypes.PERCENT, OrderTypes.TARGET_WEIGHT)

"""
The OrderbookActor is responsible to keep track of orders the client places. 
It needs a Portfolio Actor to tell in case an order is executed.

The Actor accepts the following messages:
 * a message to place an order in percentages (weights) or quantity (nr of shares)
 * a message which tells the orderbook about new market quote updates

The actor sends the following messages:
 * asks the Portfolio Actor about the current total portfolio value   
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
                return self.place_order(OrderTypes.QUANTITY, asset, limit, stop_limit, valid_from, valid_until, qty)
            case TargetQuantityOrderMessage(asset, limit, stop_limit, valid_from, valid_until, qty):
                return self.place_order(OrderTypes.TARGET_QUANTITY, asset, limit, stop_limit, valid_from, valid_until, qty)
            case PercentOrderMessage(asset, limit, stop_limit, valid_from, valid_until, percent):
                return self.place_order(OrderTypes.PERCENT, asset, limit, stop_limit, valid_from, valid_until, percent)
            case TargetWeightOrderMessage(asset, limit, stop_limit, valid_from, valid_until, weight):
                return self.place_order(OrderTypes.TARGET_WEIGHT, asset, limit, stop_limit, valid_from, valid_until, weight)
            case CloseOrderMessage(asset, limit, stop_limit, valid_from, valid_until):
                return self.place_order(OrderTypes.CLOSE, asset, limit, stop_limit, valid_from, valid_until, None)

            # when a new quote messages comes in whe need to check if an order is executed or can be evicted.
            # if an order can be executed and the quantity is not clear (weight/percentage/amount orders)
            # we need to ask the portfolio first what the current portfolio value is
            case QuoteAskMarketData(asset, as_of, quote):
                return self.new_market_data(asset, as_of, quote, quote, quote, quote, quote, quote)
            case NewBidAskMarketData(asset, as_of, bid, ask):
                return self.new_market_data(asset, as_of, bid, ask, bid, ask, bid, ask)
            case NewBarMarketData(asset, as_of, open, high, low, close):
                return self.new_market_data(asset, as_of, open, open, high, low, close, close)
            case _:
                raise ValueError(f"Unknown Message {message}")

    @abstractmethod
    def place_order(self, order_type, asset, limit, stop_limit, valid_from, valid_until, qty):
        raise NotImplemented

    def new_market_data(self, asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask):
        # check if we have an order and if an order would be executed (or could be evicted)
        evict = self._get_orders_for_eviction(asset, as_of)
        execute = self._get_orders_for_execution(asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask)

        # evict orders and return if nothing to execute
        self._evict_orders(evict)
        if len(execute) <= 0: return

        # check if we have orders which need the portfolio value to be executable. And sort such that we sell first
        # before we increase positions
        need_portfolio_value = any([True for o in execute if "ordertype" in RELATIVE_ORDER_TYPES])
        if need_portfolio_value:
            pv: PortfolioValue = self.portfolio_actor.ask(PortfolioValueMessage()) if need_portfolio_value else None
            execute = sorted(execute, key=lambda x: x.delta(pv.positions[x.asset]))
        else:
            pv = None

        # we have some orders to execute
        for order in execute:
            match order:
                case CloseOrder(asset):
                    # if ordertype == CLOSE: execute as is
                    return
                case QuantityOrder(asset, size):
                    # if ordertype == QUANTITY: execute as is
                    return
                case TargetQuantityOrder(asset, size):
                    # if ordertype == TARGET_QUANTITY: get portfolio value and execute target_guantiuty - current quantity
                    return
                case PercentOrder(asset, size):
                    # if ordertype == PERCENT: get portfolio value cash and execute cash * percent
                    return
                case TargetWeightOrder(asset, size):
                    # if ordertype == TARGET_WEIGHT: get portfolio value cash and execute cash * percent
                    return
                case _ :
                    raise ValueError(f"Unknown Order: {order}")

        # finally evict all orders we have executed from the orderbook
        self._evict_orders(execute)

    @abstractmethod
    def _get_orders_for_eviction(self, asset, as_of) -> List[Order]:
        raise NotImplemented

    @abstractmethod
    def _get_orders_for_execution(self, asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask) -> List[Order]:
        raise NotImplemented

    @abstractmethod
    def _evict_orders(self, orders: List[Order]):
        raise NotImplemented


class MemoryOrderbookActor(AbstractOrderbookActor):

    def __init__(self, portfolio_actor: pykka.ActorRef):
        super().__init__(portfolio_actor)

    def place_order(self, order_type, asset, limit, stop_limit, valid_from, valid_until, qty):
        # simply store the order in the datastructure i.e. sqlite
        pass

    def new_market_data(self, asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask):
        pass
