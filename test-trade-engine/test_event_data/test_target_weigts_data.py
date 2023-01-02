from unittest import TestCase

import pytest

from tradeengine.events import TargetWeights, Asset


class TestTargetWeightsData(TestCase):

    def test_weights(self):
        tw = TargetWeights((["A", "B"], [0.4, 0.6]))
        self.assertEqual(
            {Asset('A'): 0.4, Asset('B'): 0.6},
            tw.asset_weights
        )

    def test_weights_inavlid(self):
        with pytest.raises(AssertionError):
            TargetWeights((["A", "B"], [0.6, 0.6]))

        with pytest.raises(AssertionError):
            TargetWeights((["A", "B"], [1.1, -0.2]))

        with pytest.raises(AssertionError):
            TargetWeights((["A", "B"], [-0.6, -0.6]))

        with pytest.raises(AssertionError):
            TargetWeights((["A", "B"], [-1.2, 0.2]))