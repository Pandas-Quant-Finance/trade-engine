from pathlib import Path

import pandas as pd

from tradeengine.dto.dataflow import Asset

AAPL_MD_FRAMES = {
    Asset(ticker): pd.read_csv(Path(__file__).parents[1].joinpath(f"{ticker.lower()}.csv"), parse_dates=True, index_col="Date").sort_index()
    for ticker in ["AAPL"]
}

AAPL_MSFT_MD_FRAMES = {
    Asset(ticker): pd.read_csv(Path(__file__).parents[1].joinpath(f"{ticker.lower()}.csv"), parse_dates=True, index_col="Date").sort_index()
    for ticker in ["AAPL", "MSFT"]
}

AAPL_MSFT_TLT_MD_FRAMES = {
    Asset(ticker): pd.read_csv(Path(__file__).parents[1].joinpath(f"{ticker.lower()}.csv"), parse_dates=True, index_col="Date").sort_index()
    for ticker in ["AAPL", "MSFT", "TLT"]
}

ASSETS = list[AAPL_MSFT_TLT_MD_FRAMES.keys()]
AAPL = Asset("AAPL")
MSFT = Asset("MSFT")
TLT = Asset("MSFT")
