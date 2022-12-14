### Usage
The trade engine is meant to execute trades after a trading signal occurred. 
This can be a proxy object to a broker api, or it can be used to simulate a past behavior (backtest).

In the case of back testing note that you can not execute on the same price where your signal was generated 
but only on the very next tick. For end of day data this would mean at the open of the next bar, because 
after the current bar the market is closed already. Only if you have market data for after hours of course 
you could/should use that data.


```python
from tradeengine import YFinanceBacktestingTradeEngine
from datetime import datetime

trade_engine = YFinanceBacktestingTradeEngine()

# .. execute some trades
trade_engine.trade("AAPL", 10, timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Long")
trade_engine.close('AAPL', timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")

# or
trade_engine.trade("AAPL", -10, timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Long")
trade_engine.close('AAPL', timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")

# or
trade_engine.trade("AAPL", 10, timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Long")
trade_engine.trade('AAPL', -10, timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")

# or
trade_engine.trade("AAPL", 'max', timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Long")
trade_engine.close('AAPL', timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")

# or
trade_engine.target_weights(['AAPL', 'MSFT'], [0.4, 0.6], timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")
trade_engine.close('AAPL', timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")

# get results
trade_engine.get_history()
```

In order to obtain classical risk measurements like sharpe ratio you could 
install quantstats `pip install QuantStats` and apply on the portfolio 
returns like so.

```python
import quantstats as qs

# get some statistics
qs.stats.sharpe(trade_engine.get_history()[("TOTAL", "return")])
```

### Use Measurements for Reinforcement Learning
Eventually you could wrap the whole `TradeEngine` into an OpenAI Gym Environment 
following this guide: https://www.gymlibrary.dev/content/environment_creation/
