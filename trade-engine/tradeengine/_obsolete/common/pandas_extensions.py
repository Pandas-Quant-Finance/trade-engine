from pandas.core.base import PandasObject


def cumpct_change(df):
    return ((df.pct_change() + 1).cumprod() - 1)


PandasObject.cumpct_change = cumpct_change