from tradeengine.dto.dataflow import CASH


def get_color_for(asset):
    import plotly

    if asset in (CASH, "$$$", "Portfolio"):
        return '#555555'

    color_scale = plotly.colors.qualitative.Light24 + plotly.colors.qualitative.Dark24
    return color_scale[hash(asset) % len(color_scale)]
