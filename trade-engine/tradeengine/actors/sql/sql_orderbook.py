import logging
from datetime import timedelta, datetime
from typing import List, Tuple, Callable

import pykka
from sqlalchemy import Engine, text, select, func, update, delete, and_, or_, between, case, null
from sqlalchemy.orm import Session

from tradeengine.actors.orderbook_actor import AbstractOrderbookActor
from tradeengine.actors.sql.persitency import OrderBookBase, OrderBook
from tradeengine.dto.dataflow import Order, QuantityOrder, PortfolioValue, OrderTypes, Asset, CloseOrder, PercentOrder, \
    TargetQuantityOrder, TargetWeightOrder, ExpectedExecutionPrice

LOG = logging.getLogger(__name__)


class SQLOrderbookActor(AbstractOrderbookActor):

    def __init__(
            self,
            portfolio_actor: pykka.ActorRef,
            alchemy_engine: Engine,
            fee_calculator: Callable[[float, float], float] = lambda qty, price: 0,
            relative_order_min_impact: float = 0,
            slippage: float = 0,
            strategy_id: str = ''
    ):
        super().__init__(portfolio_actor)
        self.engine = alchemy_engine
        self.strategy_id = strategy_id

        self.fee_calculator = fee_calculator
        self.relative_order_min_impact = relative_order_min_impact
        self.slippage = slippage

        LOG.info("generate OrderBook database objects")
        OrderBookBase.metadata.create_all(bind=alchemy_engine)

    def on_stop(self) -> None:
        try:
            # close database connection
            self.engine.dispose()
        except Exception as e:
            LOG.error(e)
        finally:
            super().on_stop()

    def place_order(self, order: Order):
        # simply store the order in the datastructure i.e. sqlite
        with Session(self.engine) as session:
            session.add(
                OrderBook(
                    strategy_id=self.strategy_id,
                    order_type=order.type,
                    asset=order.asset,
                    limit=order.limit,
                    stop_limit=order.stop_limit,
                    valid_from=order.valid_from,
                    valid_until=order._valid_until(),
                    qty=order.size
                )
            )
            session.commit()

    def get_full_orderbook(self):
        with Session(self.engine) as session:
            return list(session.scalars(select(OrderBook).where(OrderBook.strategy_id == self.strategy_id)))

    def _evict_orders(self, asset: Asset, as_of: datetime) -> int:
        # delete orders where valid_until < as_of from the orderbook and put it to the orderbook_history
        # returns the number of evicted orders
        evicted = 0
        with Session(self.engine) as session:
            for order in session.scalars(
                select(OrderBook)\
                    .where((OrderBook.strategy_id == self.strategy_id) & (OrderBook.valid_until < as_of))
            ):
                session.delete(order)
                session.add(order.to_history(0, None))
                evicted += 1

            session.commit()

        return evicted

    def _get_orders_for_execution(self, asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask) -> List[Order]:
        def map_order(o: OrderBook) -> Order:
            match o.order_type:
                case OrderTypes.CLOSE:
                    return CloseOrder(o.asset, o.qty, o.valid_from, o.limit, o.stop_limit, o.valid_until, o.id)
                case OrderTypes.QUANTITY:
                    return QuantityOrder(o.asset, o.qty, o.valid_from, o.limit, o.stop_limit, o.valid_until, o.id)
                case OrderTypes.TARGET_QUANTITY:
                    return TargetQuantityOrder(o.asset, o.qty, o.valid_from, o.limit, o.stop_limit, o.valid_until, o.id)
                case OrderTypes.PERCENT:
                    return PercentOrder(o.asset, o.qty, o.valid_from, o.limit, o.stop_limit, o.valid_until, o.id)
                case OrderTypes.TARGET_WEIGHT:
                    return TargetWeightOrder(o.asset, o.qty, o.valid_from, o.limit, o.stop_limit, o.valid_until, o.id)

        with Session(self.engine) as session:
            sql = _get_executable_orders_from_orderbook_sql(self.strategy_id, asset, as_of, high, low)
            return [map_order(o) for o in session.scalars(sql)]

    def _execute_order(self, order: QuantityOrder, expected_price: float, pv: PortfolioValue | None) -> Tuple[float | None, float | None, float | None]:
        # delete fully filled orders from the orderbook and put it to the orderbook_history
        # return the definitive traded quantity, price and fee, in case we only want to execute orders which a
        # strong enough portfolio impact (>= x% of portfolio value) we can abort the execution and return None values.

        with Session(self.engine) as session:
            # later we may want to implement partial execution, for now we execute everything as is and move to history
            for o in session.scalars(
                select(OrderBook)\
                    .where((OrderBook.strategy_id == self.strategy_id) & (OrderBook.id == order.id))
            ):
                session.delete(o)
                session.add(o.to_history(1, expected_price))

            session.commit()

        price = expected_price * (1 + self.slippage)
        impact = (order.size * price) / pv.value() if pv is not None else 0
        fee = self.fee_calculator(order.size, price)

        if pv is not None and self.relative_order_min_impact > 0 and impact < self.relative_order_min_impact:
            LOG.info(f"evicting non impactful order {impact} < {self.relative_order_min_impact}")
            return None, None, None

        return order.size, price, fee


def _get_executable_orders_from_orderbook_sql(strategy_id, asset, as_of, low, high):
    # all orders where valid_from >= as_of and valid_until >= as_of and where the limit is matched
    # return fifo
    return select(OrderBook) \
        .where(
            and_(
                OrderBook.strategy_id == strategy_id,
                OrderBook.asset == asset,
                between(as_of, OrderBook.valid_from, OrderBook.valid_until),
                or_(
                    OrderBook.limit == null(),
                    case(
                        (and_(OrderBook.qty < 0, OrderBook.limit >= low), 1),
                        (and_(OrderBook.qty >= 0, OrderBook.limit <= high), 1),
                        else_=0
                    ) == 1,
                    case(
                        (and_(OrderBook.qty < 0, OrderBook.stop_limit >= low), 1),
                        (and_(OrderBook.qty >= 0, OrderBook.stop_limit <= high), 1),
                        else_=0
                    ) == 1,
                )
            )
        )\
        .order_by(OrderBook.valid_from)
