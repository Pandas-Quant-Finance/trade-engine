import hashlib

import plotly

color_scale = plotly.colors.qualitative.Light24 + plotly.colors.qualitative.Dark24


def get_color_for(asset):
    if asset in ("$$$", "Portfolio"):
        return '#555555'

    asset_hash = int(hashlib.md5(str(asset).encode("utf-8")).hexdigest(), 16)
    return color_scale[asset_hash % len(color_scale)]
