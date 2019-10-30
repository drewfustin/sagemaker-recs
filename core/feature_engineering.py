import pandas as pd
from pandas import DataFrame, Series
from typing import List


def get_zip_region(df: DataFrame) -> Series:
    available_regions: List[str] = (
        [str(n).zfill(2) for n in range(100)] +
        ["E2", "K7", "L1", "L9", "M4", "M7", "N2", "N4", "R3", "T8", "V0", "V1", "V3", "V5", "Y1"]
    )
    return pd.Categorical(df["zip_code"].str[:2], categories=available_regions, ordered=True)


def get_movie_age(df: DataFrame) -> Series:
    return (df["timestamp"] - df["release_date"]).dt.days / 365
