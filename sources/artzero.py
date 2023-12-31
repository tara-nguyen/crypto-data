import pandas as pd
from reports.quarterly_etl import extract


class ArtzeroExtractor:
    """Extract data from ArtZero API."""

    def __init__(self, route):
        self.method = "POST"
        self.url = f"https://astar-api.artzero.io{route}"
        self.headers = {"Content-Type": "application/x-www-form-urlencoded"}

    def extract(self, payload):
        data = extract(self.method, self.url, headers=self.headers,
                       data=payload)["ret"]

        return data


class ArtzeroTransformer:
    """Convert json-encoded content to a dataframe."""

    def __init__(self, data):
        self.data = data

    def to_frame(self):
        df = pd.DataFrame(self.data)

        return df
