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


def convert_timestamp(t, timestamp_format=None, stop=10):
    if isinstance(t, str):
        new_t = t[:stop]
    else:
        new_t = pd.to_datetime(
            t, unit="ms" if isinstance(t, (int, float)) else None)
        new_t = new_t.strftime(timestamp_format)

    return new_t


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
