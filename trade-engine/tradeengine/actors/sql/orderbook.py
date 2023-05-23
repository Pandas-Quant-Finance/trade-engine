from tradeengine.actors.orderbook_actor import AbstractOrderbookActor


class SQLOrderbookActor(AbstractOrderbookActor):

    def place_order(self, order_type, asset, limit, stop_limit, valid_from, valid_until, qty):
        # simply store the order in the datastructure i.e. sqlite
        pass

    def new_market_data(self, asset, as_of, open_bid, open_ask, high, low, close_bid, close_ask):
        # check if we have an order and if an order would be executed (or could be evicted)
        # if it can be executed eventually ask the portfolio for needed data first
        pass

