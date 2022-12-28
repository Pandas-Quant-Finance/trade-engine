from unittest import TestCase

from tradeengine.components import Account
from tradeengine.events import *


class TestAccountComponent(TestCase):

    def test_account(self):
        a = Account()

        a.place_maximum_order(MaximumOrder("AAPL", valid_from='2020-01-01'))

    def test_place_target_weights_oder(self):
        pass


    def test_get_history(self):
        pass

