from typing import Generator

import pandas as pd

from tradeengine.common.tz_compare import timestamp_less_equal


class DataFrameIterator(object):

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.idx = df.index.tolist()

    def next_until(self, time) -> Generator[pd.Series, pd.Series, None]:
        while len(self.idx) > 0 and timestamp_less_equal(self.idx[0], time):
            yield self.df.loc[self.idx.pop(0)]
