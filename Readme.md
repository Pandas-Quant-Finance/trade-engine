### Getting Risk Measures
In order to obtain classical risk measurements like sharpe ratio you could 
install quantstats `pip install QuantStats` and apply on the portfolio 
returns like so.

```python
import quantstats as qs
from tradeengine import YFinanceBacktestingTradeEngine
from datetime import datetime

trade_engine = YFinanceBacktestingTradeEngine()

# .. execute some trades
trade_engine.trade("AAPL", 10, timestamp=datetime.fromisoformat('2020-01-01'), position_id="APPL-Long")
trade_engine.close('AAPL', timestamp=datetime.fromisoformat('2020-01-06'), position_id="APPL-Long")

qs.stats.sharpe(trade_engine.get_history()[("TOTAL", "return")])
```

### Use Measurements for Reinforcement Learning
Eventually you could wrap the whole `TradeEngine` into an OpenAI Gym Environment 
following this guide: https://www.gymlibrary.dev/content/environment_creation/
