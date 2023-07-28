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


def to_epoch(time):
    """Convert a time string to epoch time (i.e. the number of seconds from
    1970-01-01 00:00:00).
    """
    time = pd.Timestamp(time) - pd.Timestamp(0)
    time = int(np.ceil(time.total_seconds()))

    return time


def get_token_amount(x, network):
    """Convert a hex string or a character string containing only digits to an
    integer. Then, divide the integer by the denomination of the given network's
    token.
    """
    if isinstance(x, str):
        if x.isdigit() or "." in x:
            x = float(x)
        else:
            x = int(x, 16)

    networks = QuarterlyReport().networks
    if network in networks:
        x /= networks[network]
    else:
        raise Exception(
            f'network must be either "{networks[0]}" or "{networks[1]}"')

    return x


def print_long_df(df, head_len=10, tail_len=None):
    if tail_len is None:
        tail_len = head_len
    if df.shape[0] > head_len + tail_len:
        print(pd.concat([df.head(head_len), df.tail(tail_len)]).to_string())
    else:
        print(df.to_string())
