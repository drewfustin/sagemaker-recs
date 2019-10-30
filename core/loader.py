import numpy as np
import pandas as pd
from pandas import DataFrame


def load_data(s3_bucket: str, s3_key: str) -> DataFrame:
    allowed_keys = (
        ["u.data", "u.item", "u.user"] +
        list(np.array([(f"u{i}.base", f"u{i}.test")
                       for i in [str(n) for n in range(1, 6)] + ["a", "b"]]).flatten()))
    assert s3_key in allowed_keys, \
        f"Don't look at that data. Only look at one of these: {allowed_keys}"
    col_names = {
        "data": ["user_id", "movie_id", "rating", "timestamp"],
        "base": ["user_id", "movie_id", "rating", "timestamp"],
        "test": ["user_id", "movie_id", "rating", "timestamp"],
        "item": ["movie_id", "movie_title", "release_date", "video_release_date",
                 "imdb_url", "unknown", "action", "adventure", "animation",
                 "childrens", "comedy", "crime", "documentary", "drama", "fantasy",
                 "film_noir", "horror", "musical", "mystery", "romance", "sci_fi",
                 "thriller", "war", "western"],
        "user": ["user_id", "age", "gender", "occupation", "zip_code"],
    }
    occupations = ["administrator", "artist", "doctor", "educator", "engineer",
                   "entertainment", "executive", "healthcare", "homemaker", "lawyer",
                   "librarian", "marketing", "none", "other", "programmer", "retired",
                   "salesman", "scientist", "student", "technician", "writer"]
    dtypes = {
        "user_id": int,
        "movie_id": int,
        "rating": pd.CategoricalDtype(categories=[1, 2, 3, 4, 5], ordered=True),
        "timestamp": str,
        "movie_title": str,
        "release_date": str,
        "video_release_date": str,
        "imdb_url": str,
        "unknown": bool,
        "action": bool,
        "adventure": bool,
        "animation": bool,
        "childrens": bool,
        "comedy": bool,
        "crime": bool,
        "documentary": bool,
        "drama": bool,
        "fantasy": bool,
        "film_noir": bool,
        "horror": bool,
        "musical": bool,
        "mystery": bool,
        "romance": bool,
        "sci_fi": bool,
        "thriller": bool,
        "war": bool,
        "western": bool,
        "age": int,
        "gender": pd.CategoricalDtype(categories=["M", "F"], ordered=False),
        "zip_code": str,
        "occupation": pd.CategoricalDtype(categories=occupations, ordered=False),
    }

    def parse_dates(df: DataFrame) -> DataFrame:
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        for col in set(['release_date', 'video_release_date']).intersection(df.columns):
            df[col] = pd.to_datetime(df[col], format="%d-%b-%Y")
        return df

    return (pd.read_csv(f"s3://{s3_bucket}/{s3_key}",
                        encoding="ISO-8859-1",
                        sep="|" if s3_key.split('.')[-1] in ('item', 'user') else "\t",
                        header=None,
                        names=col_names[s3_key.split('.')[-1]],
                        dtype={col: dtypes[col] for col in col_names[s3_key.split('.')[-1]]})
              .pipe(parse_dates)
            )


def load_merged_data(s3_bucket: str) -> DataFrame:
    df_data = load_data(s3_bucket, "u.data")
    df_item = load_data(s3_bucket, "u.item")
    df_user = load_data(s3_bucket, "u.user")
    return (pd.merge(pd.merge(df_data, df_item, on="movie_id"), df_user, on="user_id")
              .set_index(["user_id", "movie_id"]))
