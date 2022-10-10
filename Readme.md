### Getting Risk Measures
In order to obtain classical risk measurements like sharpe ratio you could 
install quantstats `pip install QuantStats` and apply on the portfolio 
returns like so.

```python
import quantstats as qs
from tradeengine import YFinanceBacktestingTradeEngine

trade_engine = YFinanceBacktestingTradeEngine()

# .. execute some trades

qs.stats.sharpe(trade_engine.get_history()[("TOTAL", "return")])
```

### Use Measurements for Reinforcement Learning
Eventually you could wrap the whole `TradeEngine` into an OpenAI Gym Environment 
following this guide: https://www.gymlibrary.dev/content/environment_creation/
