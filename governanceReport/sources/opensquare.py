import pandas as pd
from reports.governance_etl import (GovernanceReport, get_token_amount,
                                    convert_timestamp)
from requests import Session


class Extractor:
    def __init__(self, source, network, route):
        self.network = network.lower()
        networks = list(GovernanceReport().networks.keys())
        if self.network not in networks:
            raise Exception(
                f'network must be either "{networks[0]}" or "{networks[1]}"')

        if source.lower() == "subsquare":
            self.url = f"https://{self.network}.subsquare.io/api{route}"
        elif source.lower() == "dotreasury":
            self.url = f"https://api.dotreasury.com/{self.network}{route}"
        else:
            raise Exception('source must be either "subsquare" or "dotreasury"')

    def extract(self):
        """Query from an online source and return the json-encoded content of
         the response.
         """
        data = Session().get(self.url).json()
        if "pageSize" in data:
            resp = Session().get(self.url, params={"page_size": data["total"]})
            data = resp.json()["items"]

        return data


class Transformer:
    def __init__(self, data):
        self.data = data

    def transform(self, fields=None, token_cols=None, network=None,
                  time_cols=None, sort=True, sort_by=None, **kwargs):
        df = pd.json_normalize(self.data, sep="_")
        if fields is not None:
            df = df.reindex(columns=fields)

        if token_cols is not None:
            df[token_cols] = df[token_cols].applymap(
                lambda x: get_token_amount(x, network), na_action="ignore")
        if time_cols is not None:
            df[time_cols] = df[time_cols].applymap(
                lambda t: convert_timestamp(t), na_action="ignore")

        if sort:
            if sort_by is None:
                if fields[0] == "account":
                    df = df.sort_values(fields[0], key=lambda s: s.str.lower())
                else:
                    df = df.sort_values(fields[0], ascending=False)
            else:
                df = df.sort_values(sort_by, **kwargs)

        return df
