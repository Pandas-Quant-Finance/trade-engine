from pathlib import Path

import pandas as pd

from tradeengine.dto.dataflow import Asset

FRAMES = {
    Asset(ticker): pd.read_csv(Path(__file__).parents[1].joinpath(f"{ticker.lower()}.csv"), parse_dates=True, index_col="Date").sort_index()
    for ticker in ["AAPL", "MSFT"]
}

ASSETS = list[FRAMES.keys()]
AAPL = Asset("AAPL")
MSFT = Asset("MSFT")
