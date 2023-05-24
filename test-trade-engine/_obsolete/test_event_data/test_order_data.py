from unittest import TestCase

from tradeengine._obsolete.events import Order


class TestOrderData(TestCase):

    def test_tick_valid_to(self):
        o = Order("AAPL", 10, valid_to=2)
        self.assertTrue(o.valid_after_subtract_tick())
        self.assertFalse(o.valid_after_subtract_tick())

