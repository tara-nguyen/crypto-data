import pandas as pd
from reports.quarterly_etl import extract


class ParasExtractor:
    """Extract data from Paras API."""

    def __init__(self):
        self.method = "GET"
        self.url = f"https://api.paras.id/marketplace/activities"

    def extract(self, params):
        data = extract(self.method, self.url, params=params)["data"]["results"]

        return data


class ParasTransformer:
    """Convert json-encoded content to a dataframe."""

    def __init__(self, data):
        self.data = data

    def to_frame(self):
        df = pd.json_normalize(self.data, sep="_")

        return df
