from typing import Dict, Literal

import numpy as np
import pandas as pd

from testutils.data import AAPL_MSFT_MD_FRAMES
from tradeengine.dto.dataflow import Asset, TargetWeightOrder, PercentOrder, CloseOrder


def sample_strategy(frames: Dict[Asset, pd.DataFrame], stragegy: Literal['long', 'short', 'swing'] = 'swing', signal_only=True, slow=90, fast=20) -> Dict[Asset, pd.Series | pd.DataFrame]:
    result = {}
    size = 1 / len(frames)

    for a, f in frames.items():
        df = f[["Close"]]
        df["ma_fast"] = df["Close"].rolling(fast).mean()
        df["ma_slow"] = df["Close"].rolling(slow).mean()
        df["signal"] = df["ma_fast"] - df["ma_slow"]
        df["signal"] = df["signal"].rolling(2).apply(lambda x: (np.sign(x[0]) != np.sign(x[1])) * np.sign(x[1])).fillna(0)

        if stragegy == 'swing':
            df["order"] = df["signal"].apply(lambda x: {TargetWeightOrder: dict(size=size if x > 0 else -size)} if x != 0 else None)
        elif stragegy == 'short':
            df["order"] = df["signal"].apply(lambda x: {TargetWeightOrder: dict(size=-size)} if x < 0 else {CloseOrder: {}} if x > 0 else None)
        else:
            df["order"] = df["signal"].apply(lambda x: {(PercentOrder if len(frames) == 1 else TargetWeightOrder): dict(size=size)} if x > 0 else {CloseOrder: {}} if x < 0 else None)

        result[a] = df["order"] if signal_only else df

    return result


def one_over_n(frames: Dict[Asset, pd.DataFrame],) -> Dict[Asset, pd.Series | pd.DataFrame]:
    size = 0.98 / len(frames)
    result = {}

    for a, f in frames.items():
        result[a] = pd.Series(size, index=f.index).apply(lambda x: {TargetWeightOrder: dict(size=x)})

    return result
