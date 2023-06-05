import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype


def frames_allmost_equal(left, right, strict=True):
    np.testing.assert_array_almost_equal(
        left[[col for col in left.columns if is_numeric_dtype(left[col].dtype)]].values,
        right[[col for col in right.columns if is_numeric_dtype(right[col].dtype)]].values,
    )

    if strict:
        pd.testing.assert_frame_equal(left, right)