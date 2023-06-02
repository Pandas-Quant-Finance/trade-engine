## Python Trade Engine

The Python Trade Engine Application aims to simulate a real world implementation of
any given trading strategy. The goal is to use the same mechanism as production close 
as possible for backtesting and live implementation. This allows us to place orders
in any scenario (live trading or backtesting) with the usual capabilities like 
stop orders, limit orders, one cancels the other, etc.

### Architecture
The application is implemented using the actor model. We have three actors which play 
together:
 
 * Portfolio
 * Orderbook
 * Market Data

#### The Portfolio Actor
The portfolio actor is the bookie of the portfolio managing the positions, evaluations,
profit and loss, historical timeseries, ...

#### The Orderbook Actor
The orderbook actor is responsible for keeping track of its orders which means he 
mainly has to:
 * evict unfilled orders
 * notify the portfolio actor if an order got filled

based on incoming market data.

#### The Market Data Actor
The last actor in the system is the market data actor. He has to emit strictly and 
chronologically price updates of all assets we want to trade. The market data actor
always informs the portfolio actor first on any new price update. This guarantees that
the orderbook actor has the most recent portfolio valuation possible in order to derive
orders which are not of discrete units but like % of capital orders.

NOTE If we want to dynamically trade new assets the user needs to take care of 
subscribing and un-subscribing to the needed market data feeds.

For backtest all quotes have to be emitted chronologically first and then for each asset.


### Backtesting
For backtesting in order to guarantee no lookahead bias the `backtest` API is recommended.
The backtest API need a Market Data Actor which is capable to respond to the
`ReplayAllMarketDataMessage` message. The Actor has to be strict chronologically and send
market data for one asset after the other for the same timestamp.

```python
import pandas as pd
from sqlalchemy import create_engine, StaticPool
from tradeengine.actors.memory import MemPortfolioActor
from tradeengine.actors.sql import SQLOrderbookActor
from tradeengine.backtest import backtest_strategy
from tradeengine.dto import Asset, PercentOrder
import yfinance as yf

aapl = yf.Ticker("AAPL").history("max")
market_data = {
    Asset("AAPL"): aapl
}
buy_and_hold_signal = {
    Asset("AAPL"): pd.Series([[{PercentOrder: dict(size=1.0)}]] + [[]] * (len(aapl) - 1), index=aapl.index)
}

portfolio_actor = MemPortfolioActor.start(funding=100)
backtest = backtest_strategy(
    SQLOrderbookActor.start(
        portfolio_actor,
        create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool),
        strategy_id='my-strategy'
    ),
    portfolio_actor,
    market_data,
    buy_and_hold_signal,
)
```

The `backtest_strategy` returns a `Backtest` object which is just a dataclass holding a bunch
of pandas DataFrames:

```
@dataclass(frozen=True, eq=True)
class Backtest:
    market_data: pd.DataFrame
    signals: pd.DataFrame
    orders: pd.DataFrame
    position_values: pd.DataFrame
    position_weights:pd.DataFrame
    porfolio_performance: pd.DataFrame
    market_data_extra_data: pd.DataFrame = pd.DataFrame({})
```

In order to get some plots you can use the `dash` app or implement your own plots from the dataframes
provided.


There are some examples in the [test_actor_system](./test-trade-engine/test_actor_system) 
module.

### Production
In order to take strategies into production you need to subclass all Actors to fit
your brokers APIs.
