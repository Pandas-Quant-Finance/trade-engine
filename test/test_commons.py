from unittest import TestCase

from common.nullsafe import coalesce


class TestCommonUtils(TestCase):

    def test_coalesce(self):
        self.assertEqual(12, coalesce(None, None, 12))
        self.assertEqual(12, coalesce(None, None, 12, 13, None))
        self.assertIsNone(coalesce(None, None, None))