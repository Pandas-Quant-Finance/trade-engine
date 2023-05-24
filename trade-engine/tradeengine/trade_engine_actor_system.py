from datetime import datetime

import pykka

from tradeengine.actors.orderbook_actor import MemoryOrderbookActor
from tradeengine.actors.portfolio_actor import SQLPortfolioActor
from tradeengine.actors.market_data_actor import CSVQuoteProviderActor
from tradeengine.dto.dataflow import Asset
from tradeengine.messages.messages import PercentOrderMessage

if __name__ == "__main__":
    #adder = Adder.start().proxy()
    #bookkeeper = Bookkeeper.start(adder).proxy()
    #bookkeeper.count_to(10).get()

    # first we need a portfolio actor which keeps track of the current portfolio
    portfolio_manager = SQLPortfolioActor.start(100_000)

    # then we need an Orderbook
    orderbook = MemoryOrderbookActor.start(portfolio_manager)

    # then we need a quote provider which can response quote queries and/or publish quote update messages
    # which possibly could come from a streaming source
    quote_provider = CSVQuoteProviderActor.start(portfolio_manager, orderbook, ["some_csv_files"])

    # now we can submit orders to the orderbook
    orderbook.tell(PercentOrderMessage(Asset("AAPL"), None, None, datetime.fromisoformat('2030-12-31'), 0.5))

    # to execute some orders we need to simulate market data streaming into the orderbook
    # since this is a backtest scenario using csv data, we tell the actor to now replay the full history of market data
    quote_provider.tell("Replay all Quotes")
    portfolio_manager.ask("timeseries")

    pykka.ActorRegistry.stop_all()