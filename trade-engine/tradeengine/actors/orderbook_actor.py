from abc import abstractmethod
from datetime import datetime
from functools import partial
from typing import Any, List, Tuple

import pykka

from tradeengine.dto.dataflow import OrderTypes, PortfolioValue, Order, QuantityOrder, \
    Asset
from tradeengine.messages.messages import NewBidAskMarketData, NewBarMarketData, PortfolioValueMessage, \
    NewPositionMessage, NewOrderMessage


RELATIVE_ORDER_TYPES = (OrderTypes.TARGET_QUANTITY, OrderTypes.PERCENT, OrderTypes.TARGET_WEIGHT)


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

    def on_receive(self, message: Any) -> Any:
        match message:
            # if message is PlaceOrder, we store the order in the orderbook
            case NewOrderMessage(order):
                return self.place_order(order)

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
        self._evict_orders(asset, as_of)

        # check if we have an order and if an order would be executed and return if nothing to execute
        executable_orders = self._get_orders_for_execution(asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask)
        if len(executable_orders) <= 0: return

        need_portfolio_value = any(o.type for o, _ in executable_orders if o.type in [RELATIVE_ORDER_TYPES])
        pv: PortfolioValue = self.portfolio_actor.ask(PortfolioValueMessage()) if need_portfolio_value else None

        # sort orders by sell orders first:
        for executable_order in sorted(executable_orders, key=partial(order_sorter, pv=pv)):
            self._execute_executable_order(*executable_order, asset, as_of)

    def _execute_executable_order(self, order: Order, expected_price: float, asset: Asset, as_of: datetime):
        # check if we have orders which need the portfolio value to be executable. And sort such that we sell first
        # before we increase positions
        need_portfolio_value = order.type in [RELATIVE_ORDER_TYPES]
        pv: PortfolioValue = self.portfolio_actor.ask(PortfolioValueMessage()) if need_portfolio_value else None
        execute_order = order.to_quantity(pv, expected_price)

        # we only have one net order for one asset from one asset's price update which we execute now.
        # and then tell the portfolio actor about it
        quantity, price, fee = self._execute_order(execute_order, expected_price, pv)
        if quantity is not None:
            self.portfolio_actor.tell(NewPositionMessage(asset, as_of, quantity, price, fee))

    @abstractmethod
    def place_order(self, order: Order):
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


def order_sorter(order_with_expected_price: Tuple[Order, float], pv: PortfolioValue | None) -> int:
    order, expected_price = order_with_expected_price

    """
    we need the following order of orders
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
            return 0
        case (OrderTypes.QUANTITY, qty) if qty < 0:
            return 1
        case (OrderTypes.TARGET_QUANTITY, qty) if qty < 0:
            return 2
        case (OrderTypes.TARGET_WEIGHT, qty) if qty < 0:
            return 3
        case (OrderTypes.QUANTITY, _):
            return 4
        case (OrderTypes.TARGET_QUANTITY, _):
            return 4
        case (OrderTypes.TARGET_WEIGHT, _):
            return 4
        case (OrderTypes.PERCENT, _):
            return 5
        case _:
            return 999
