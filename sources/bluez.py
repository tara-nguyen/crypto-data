import pandas as pd
from reports.quarterly_etl import extract


class BluezExtractor:
    def __init__(self):
        self.method = "POST"
        self.url = "https://api.bluez.app/graphql"
        self.headers = {"Content-Type": "application/json"}

    def extract(self, payload):
        """Extract data from Bluez graphql."""
        data = extract(self.method, self.url, headers=self.headers,
                       data=payload)["data"]

        return data


class BluezTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self):
        df = pd.json_normalize(self.data, sep="_")

        return df
