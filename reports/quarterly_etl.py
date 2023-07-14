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


def format_timestamps(ts, timestamp_format):
    ts = [t.strftime(timestamp_format) for t in ts]

    return ts


def trim_timestamp(ts):
    ts = ts.str.slice(stop=10)

    return ts


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
