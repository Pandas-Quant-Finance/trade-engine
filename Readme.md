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


