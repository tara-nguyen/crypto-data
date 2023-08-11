import pandas as pd
from reports.quarterly_etl import extract


class NftradeExtractor:
    def __init__(self):
        self.method = "GET"
        self.url = "https://api.nftrade.com/api/v1/activities"
        self.headers = {"Referer": "https://nftrade.com/"}
        self.limit = 50

    def extract(self, skip, chain_id=1284):
        params = {"limit": self.limit, "skip": skip, "chainId": chain_id}
        data = extract(self.method, self.url, headers=self.headers,
                       params=params)

        return data


class NftradeTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self):
        df = pd.DataFrame(self.data)

        return df
