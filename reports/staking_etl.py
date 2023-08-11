import numpy as np
import pandas as pd
from requests import Session


class StakingReport:
    def __init__(self):
        self.project_name = "STAKING REPORT - 2023-JUNE"
        self.timestamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        self.start_time = pd.Timestamp(
            2022, 11, 1).strftime(self.timestamp_format)
        self.end_time = pd.Timestamp(
            2023, 6, 1).strftime(self.timestamp_format)
        self.start_era = get_era(self.start_time)
        self.end_era = get_era(self.end_time)


def to_epoch(time):
    """Convert a time string to epoch time (i.e. the number of seconds from
    1970-01-01 00:00:00.
    """
    time = pd.to_datetime(time) - pd.Timestamp(0, tz="utc")
    time = int(np.ceil(time.total_seconds()))

    return time


def get_era(time):
    """Convert a timestamp to stakingReport era ID."""
    time_diff = (
            pd.Timestamp(time, tz="utc") - pd.Timestamp(2020, 6, 21, tz="utc"))
    era = 20 + time_diff.days

    return era


def get_time(era, time_format=StakingReport().timestamp_format):
    """Convert a staking era ID to a time string with the specified format."""
    time = (pd.to_timedelta(era - StakingReport().start_era, "D")
            + pd.Timestamp(StakingReport().start_time))
    if isinstance(era, int):
        time = time.strftime(time_format)
    else:
        time = time.dt.strftime(time_format)

    return time


def extract(method, url, **kwargs):
    """Query from an online source and return the json-encoded content of the
     response.
     """
    resp = Session().request(method, url, **kwargs)
    # if not resp.ok:
    #     print("Error", resp.status_code)
    data = resp.json()

    return data


def get_daily_data(df, from_epoch, unit="ms", time_col="timestamp",
                   drop_time_col=True):
    """Filter a dataframe to retain only the last data point from each date."""
    df = df.sort_values(time_col, ascending=False)
    if from_epoch:
        df["date"] = pd.to_datetime(df[time_col], unit=unit)
    else:
        df["date"] = pd.to_datetime(df[time_col])
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df = df.groupby("date").head(1)
    # Move the "date" column to the beginning of the dataframe
    columns = df.columns[np.concatenate([[-1], np.arange(df.shape[1] - 1)])]
    df = df.reindex(columns=columns).reset_index(drop=True)
    if drop_time_col:
        df = df.drop(columns=time_col)

    return df


def transform_numeric(values, divided_by=1e10):
    """Divide a collection of values by the specified divisor(s) and return the
    new values.
    """
    values /= divided_by

    return values
