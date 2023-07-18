import numpy as np
import pandas as pd
from requests import Session


class QuarterlyReport:
    def __init__(self):
        self.project_name = "QUARTERLY REPORT - 2023-Q2"
        self.start_time = pd.Timestamp(2023, 4, 1)
        self.end_time = pd.Timestamp(2023, 7, 1)
        self.networks = {"polkadot": 1e10, "kusama": 1e12}


def extract(method, url, **kwargs):
    """Query from an online source and return the json-encoded content of the
     response.
     """
    resp = Session().request(method, url, **kwargs)
    # if not resp.ok:
    #     print("Error", resp.status_code)
    data = resp.json()

    return data


def convert_timestamp(t, timestamp_format=None, unit=None, stop=10):
    if isinstance(t, str):
        new_t = t[:stop]
    else:
        new_t = pd.to_datetime(t, unit=unit)
        if timestamp_format is not None:
            new_t = new_t.strftime(timestamp_format)

    return new_t


def to_epoch(time):
    """Convert a time string to epoch time (i.e. the number of seconds from
    1970-01-01 00:00:00.
    """
    time = pd.Timestamp(time) - pd.Timestamp(0)
    time = int(np.ceil(time.total_seconds()))

    return time
