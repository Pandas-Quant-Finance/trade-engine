from datetime import datetime, date
from unittest import TestCase

import numpy as np
import pandas as pd
import pytz

from tradeengine._obsolete.common import DataFrameIterator
from tradeengine._obsolete.common import coalesce
from tradeengine._obsolete.common import timestamp_greater


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

    def test_df_iterator(self):
        idx = pd.date_range('2000-01-01', '2001-01-01')
        df = pd.DataFrame({"X": np.random.random(len(idx))},index=idx)

        iter = DataFrameIterator(df)
        self.assertIsInstance(list(iter.next_until('2000-01-01'))[0], pd.Series)
        self.assertEqual(80, len([x for x in iter.next_until('2000-03-21')]))
        self.assertEqual(0, len([x for x in iter.next_until('2000-03-21')]))
        self.assertEqual(0, len([x for x in iter.next_until('2000-03-21')]))
        self.assertEqual(11, len([x for x in iter.next_until('2000-04-01')]))
        self.assertEqual(275, len([x for x in iter.next_until('2200-04-01')]))
        self.assertEqual(0, len([x for x in iter.next_until('2100-04-01')]))
