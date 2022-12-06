import datetime
from typing import Any, Tuple, List, Optional, Callable, Dict, Union

import numpy as np
import pandas as pd

from tradeengine.common.defaults import keydefaultdict
from tradeengine.common.nullsafe import coalesce
from tradeengine.common.tz_compare import timestamp_greater
from tradeengine.engine import TradeEngine


class BacktestingTradeEngine(TradeEngine):

    def __init__(
            self,
            next_price_func: Optional[Callable[[Any, datetime.datetime], Tuple[datetime.datetime, float, float, float, float]]] = None,
            time_series_func: Optional[Callable[[Any, datetime.datetime, datetime.datetime], pd.DataFrame]] = None,
            start_capital: float = None,
            timesteps_in_days: int = 5,
    ):
        super().__init__(start_capital)
        self.next_price_func = next_price_func
        self.time_series_func = time_series_func
        self.timesteps_in_days = timesteps_in_days
        self.positions = pd.DataFrame(
            np.zeros((0, 5)),
            columns=["pos_id", "time", "asset", "quantity", "price"]
        ).set_index(["pos_id", "time"])

    def get_all_position_ids(self) -> List[Tuple[Any, Any]]:
        current_positions = self.positions.groupby(level=0).agg({"quantity": "cumsum"}).groupby(level=0).last()
        assets = self.positions.groupby(level=0).agg({"asset": "last"})
        df = assets.join(current_positions)
        df = df[df["quantity"].abs() > 1e-6]
        return list(zip(df.index, df["asset"].tolist()))

    def get_current_position(self, position_id, *, timestamp: Optional[datetime.datetime] = None):
        if len(self.positions) <= 0 or position_id not in self.positions.index:
            return None

        if timestamp is not None:
            return self.positions.loc[position_id].loc[:timestamp, "quantity"].cumsum().iloc[-1]
        else:
            return self.positions.loc[position_id, "quantity"].cumsum().iloc[-1]

    def get_current_price(self, asset, *, timestamp: Optional[datetime.datetime] = None) -> float:
        # return self.get_next_price(asset, timestamp=timestamp)[1]
        ts_price = self.get_time_series(asset, timestamp - datetime.timedelta(days=self.timesteps_in_days), timestamp)
        return None if len(ts_price) <= 0 else ts_price.iloc[-1, -1]

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
        assert isinstance(timestamp, datetime.datetime), f"for backtesting a reference timestamp is necessary as (type datetime.datetime), {timestamp}"

        # first find the price of the next possible timestamps, exit if not available
        price_date, price = self.get_next_price(asset, timestamp, limit)
        if price is None:
            return position_id, None, None

        # as markets have a spread and fees and move we have to introduce some slippage
        price *= (1 + slippage) if quantity == 'max' or quantity > 0 else (1 - slippage)

        # let us transform an implicit max to the maximum quantity we can get
        if quantity == 'max':
            quantity = max(self.current_cash / price, 0)

        # if the quantity is 0 we obviously don't trade
        if quantity <= 1e-6:
            return position_id, 0, 0

        # increase decrease the position (coalesce(position_id, asset))
        self.update_position(coalesce(position_id, asset), asset, quantity, price, price_date)

        # return trade details
        return position_id, quantity, price

    def get_next_price(self, asset, timestamp, limit) -> Tuple[datetime.datetime, float]:
        "select * from prices where timestamp > :timestamp limit 1"
        # return coalesce(limit, price) if limit is None or high >= limit >= low else None
        if callable(self.next_price_func):
            date, open, high, low, volume = self.next_price_func(asset, timestamp)
            if date is None:
                return None, None
            else:
                return date, (abs(coalesce(limit, open)) if limit is None or high >= limit >= low else None)
        else:
            return None, None

    def get_time_series(self, asset, from_date, to_date) -> pd.DataFrame:
        if callable(self.time_series_func):
            return self.time_series_func(asset, from_date, to_date)
        else:
            raise NotImplementedError("Not implemented and no time_series_func provided")

    def update_position(self, pos_id, asset, quantity, price, price_date):
        if (pos_id, price_date) in self.positions.index:
            self.positions.drop((pos_id, price_date), axis=0, inplace=True)
            raise RecursionError("Can not trade same asset at the same time step!")

        self.positions.loc[(pos_id, price_date), :] = [asset, quantity, price]
        self.positions.sort_index(inplace=True)
        self.current_cash -= quantity * price

    def get_history(self, cash: float = None, silent: bool = False):
        cash = coalesce(cash, self.start_capital)

        # calculate pnl
        df = self.positions
        if len(df) <= 0: return None

        # position value and pnl at the time of trace
        df["trade_pos_val"] = df["quantity"] * df["price"]
        df = df.join(
            df.groupby(level=0)["quantity"].cumsum().rename("pos")
        )
        df = df.join(
            (df.groupby(level=0)["trade_pos_val"].cumsum() * -1).rename("cash_val")
        )
        df["pnl"] = df\
            .apply(lambda x: x["cash_val"] if x["pos"] == 0 else np.NaN, axis=1)

        # fill missing dates from data source
        df = pd.concat(
            [self.time_series_func(
                df.loc[asset, "asset"].iloc[0],
                df.loc[asset].index.min().to_pydatetime(),
                df.loc[asset].index.max().to_pydatetime()
            ).join(df.loc[asset], how="left") for asset in df.index.unique(level=0)],
            keys=df.index.unique(level=0),
            axis=1,
            join='outer'
        )

        df = df.swaplevel(0, 1, axis=1)
        df["pos"] = df["pos"].ffill()

        # position value and pnl during holding time
        df = df.join(
            pd.concat([df["pos"] * df["Close"]], keys=["pos_val"], axis=1)
        )
        df = df.join(
            pd.concat([df["pos_val"]], keys=["pos_pnl"], axis=1) + df["cash_val"].ffill().values
        )

        # total utilized cash and pnl of whole portfolio
        df = df.join(
            pd.concat([
                df["cash_val"].ffill().sum(axis=1).rename("TOTAL").to_frame(),
                df["pos_pnl"].sum(axis=1).rename("TOTAL").to_frame()
            ], keys=["cash_utilized", "pnl"], axis=1)
        )

        if cash is None:
            df[("cash_balance", "TOTAL")] = None
            df[("net_asset_value", "TOTAL")] = df["pos_val"].sum(axis=1)

            # Simulate an artificial purchasing power just enough to afford each position entry
            has_position = (df["pos_val"] / df["pos_val"]).replace(0, np.nan)  # 1 for open position or NaN for no pos
            virtual_cash = (df["cash_val"] * has_position).ffill().abs().sum(axis=1)
            df[("pnl_percent", "TOTAL")] = df["pos_pnl"].sum(axis=1) / virtual_cash
        else:
            df[("cash_balance", "TOTAL")] = cash + df[("cash_utilized", "TOTAL")].fillna(0)
            df[("net_asset_value", "TOTAL")] = df[("cash_balance", "TOTAL")] - df[("cash_utilized", "TOTAL")].fillna(0) + df[("pnl", "TOTAL")]
            df[("pnl_percent", "TOTAL")] = df[("pnl", "TOTAL")] / cash

            # prepend a starting balance row with only the cash provided
            df = pd.concat([
                pd.DataFrame(
                    {("cash_balance", "TOTAL"): [cash], ("net_asset_value", "TOTAL"): [cash]},
                    index=[df.index[0] - pd.Timedelta(days=1)]),
                df
            ], axis=0)

            # perform some sanity checks
            if not silent:
                # sanity check that we can't go negative
                if (df[("cash_balance", "TOTAL")] < -1e-4).sum() > 0:
                    overdraft = df[("cash_balance", "TOTAL")][(df[("cash_balance", "TOTAL")] < -1e-4)].min()
                    err = SystemError(f'Cash went negative! {overdraft}')
                    err.df = df.swaplevel(0, 1, axis=1).sort_index(axis=0).sort_index(axis=1)
                    raise err

        # finally add some total return timeseries
        pnlpercent = df[("pnl_percent", "TOTAL")] + 1
        df[("return", "TOTAL")] = (pnlpercent / pnlpercent.shift(1) - 1).fillna(0, limit=1)

        # now swap back the levels and return the detailed dataframe
        return df.swaplevel(0, 1, axis=1).sort_index(axis=0).sort_index(axis=1)


class DataFrameBacktestingTradeEngine(BacktestingTradeEngine):

    def __init__(
            self,
            dataframes: Union[Callable[[Any], pd.DataFrame], Dict[Any, pd.DataFrame]],
            start_capital: float = None,
    ):
        super().__init__(
            next_price_func=self._next_price,
            time_series_func=self._get_time_series,
            start_capital=start_capital
        )

        if callable(dataframes):
            def get_or_fetch_cached_asset(asset) -> pd.DataFrame:
                if asset not in self.timeseries:
                    self.timeseries[asset] = dataframes(asset)

                return self.timeseries[asset]

            self.timeseries = keydefaultdict(get_or_fetch_cached_asset)
        else:
            self.timeseries: Dict[Any, pd.DataFrame] = dataframes

    def _next_price(self, asset: Any, timestamp: datetime.datetime) -> Tuple[datetime.datetime, float, float, float, float]:
        df = self.timeseries[asset]
        df = df[timestamp_greater(df.index, timestamp)]
        if len(df) > 0:
            return (df.index[0].to_pydatetime(), *df[["Open", "High", "Low", "Volume"]].iloc[0].tolist())
        else:
            return None, None, None, None, None

    def _get_time_series(self, asset, from_date, to_date) -> pd.DataFrame:
        df = self.timeseries[asset]
        df = df.loc[from_date:to_date, ["Close"]]
        return df


class YFinanceBacktestingTradeEngine(DataFrameBacktestingTradeEngine):

    def __init__(self, start_capital: float = None):
        import yfinance as yf
        super().__init__(lambda asset: yf.Ticker(asset).history(period='max'), start_capital=start_capital)
