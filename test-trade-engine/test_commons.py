from datetime import datetime, date
from unittest import TestCase

import numpy as np
import pandas as pd
import pytz

from tradeengine.common.nullsafe import coalesce
from tradeengine.common.tz_compare import timestamp_greater


class TestCommonUtils(TestCase):

    def test_coalesce(self):
        self.assertEqual(12, coalesce(None, None, 12))
        self.assertEqual(12, coalesce(None, None, 12, 13, None))
        self.assertIsNone(coalesce(None, None, None))

    def test_tz_compare(self):
        tz_idx = pd.date_range('2020-01-01', '2020-12-31', tz='UTC')
        idx = pd.date_range('2020-01-01', '2020-12-31', tz=None)

        for ref_date in [
            '2020-12-28',
            datetime.fromisoformat('2020-12-28'),
            datetime.fromisoformat('2020-12-28').replace(tzinfo=pytz.UTC),
            date.fromisoformat('2020-12-28'),
        ]:
            self.assertEqual(np.sum(timestamp_greater(tz_idx, ref_date)), 3)
            self.assertEqual(np.sum(timestamp_greater(idx, ref_date)), 3)
