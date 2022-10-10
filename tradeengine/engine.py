import datetime
from abc import abstractmethod
from functools import partial
from typing import Any, Optional, List, Tuple

from common.nullsafe import is_empty_iterable, coalesce


class TradeEngine(object):

    def __init__(self, start_capital: float = None):
        super().__init__()
        self.start_capital = start_capital
        self.target_weights_residual_cash_balance = 0

    @abstractmethod
    def trade(
            self,
            asset: Any,
            quantity: float,
            *,
            limit: Optional[float] = None,
            slippage: Optional[float] = 0,
            timestamp: Optional[datetime.datetime] = None,
            position_id: Optional[Any] = None,
    ) -> Tuple[Any, float, float]:
        """
        :param asset:
        :param quantity:  < 0 = sell, > 0 = buy
        :param limit: only valid for the next timestep
        :param slippage: in percent
        :param timestamp:
        :param position_id: default asset
        :returns the position id, quantity and price at which the order was executed
        """
        pass

    @abstractmethod
    def get_all_position_ids(self) -> List[Tuple[Any, Any]]:
        """
        :returns all positions as a list of tuple of id, asset
        """
        pass

    @abstractmethod
    def get_current_position(self, position_id, *, timestamp: Optional[datetime.datetime] = None) -> float:
        pass

    @abstractmethod
    def get_current_price(self, asset, *, timestamp: Optional[datetime.datetime] = None) -> float:
        pass

    def close(self, asset: Any, *, timestamp: Optional[datetime.datetime] = None, **kwargs):
        """
        closes an existing long/short position
        :param asset:
        :param timestamp:
        :param kwargs: same keyword arguments as for `trade`
        """
        qty = self.get_current_position(kwargs.get("position_id", asset))
        if abs(qty) > 1e-6:
            self.trade(asset, qty * -1, timestamp=timestamp, **kwargs)

    def target_weights(self, assets: List[Any], weights: List[float], *, slippage: float = 0.02, timestamp: Optional[datetime.datetime] = None, silent_double_order=True, **kwargs):
        """
        increments/decrements the current position to match the desired target weight
        :param assets:
        :param weights:
        :param timestamp:
        :param kwargs:
        """

        assert sum(weights) <= 1, "Sum of weighs need to be <= 1.0"
        assert sum(weights) > 0, "Sum of weighs need to be > 0"
        assert max(weights) <= 1, "Max of weighs need to be <= 1.0"
        assert min(weights) >= -1, "Min of weighs need to be >= -1"
        assert self.start_capital is not None, "need start capital property to be present!"

        # get current positions and set weight to zero if a position is not in the target vector anymore
        # then remember ll trades and the delta weight as an ordering rank
        capital, portfolio = self.get_current_balance(timestamp=timestamp)
        position_ids = kwargs.pop("position_ids") if "position_ids" in kwargs else assets
        weights = dict(zip(position_ids, weights))
        ranked_trades = []

        for pid, ass in zip(position_ids, assets):
            quantity = coalesce(self.get_current_position(pid, timestamp=timestamp), 0.0)
            price = self.get_current_price(ass, timestamp=timestamp)
            target_pos = (capital * weights[pid]) / price
            delta_quantity = (target_pos - quantity) * (1 - slippage)

            ranked_trades.append((
                delta_quantity,
                partial(self.trade, ass, delta_quantity, timestamp=timestamp, position_id=pid, **kwargs)
            ))

        for ppid, ass in portfolio:
            # if position exists in portfolio but is not in the target vector
            if ppid not in position_ids:
                current_pos = self.get_current_position(ppid, timestamp=timestamp)
                ranked_trades.append((
                    -current_pos,
                    partial(self.trade, ass, -current_pos, timestamp=timestamp, position_id=ppid, **kwargs)
                ))

        # execute trades such that we sell first before buying in order to keep the accounting possible
        #  -> later this has to be async and parallel in case of real-time trading engine
        new_capital_employed = 0
        for _, trade in sorted(ranked_trades, key=lambda pair: pair[0]):
            try:
                _, quantity, price = trade()
                new_capital_employed += quantity * price
            except RecursionError as re:
                if not silent_double_order:
                    raise re

        # calculate difference and add to capital
        self.target_weights_residual_cash_balance -= new_capital_employed

    def get_current_balance(self, *, timestamp: Optional[datetime.datetime] = None) -> Tuple[float, List[Tuple[Any, Any]]]:
        # for current capital get all current positions and asset prices, if not present use start capital
        portfolio = self.get_all_position_ids()

        if is_empty_iterable(portfolio):
            capital = self.start_capital
            self.target_weights_residual_cash_balance = self.start_capital
        else:
            capital = self.target_weights_residual_cash_balance
            for pid, ass in portfolio:
                cpos = self.get_current_position(pid, timestamp=timestamp)
                cprice = self.get_current_price(ass, timestamp=timestamp)
                capital += cpos * cprice

        return capital, portfolio
