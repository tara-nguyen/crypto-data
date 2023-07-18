import pandas as pd
from reports.quarterly_etl import extract


class StatescanExtractor:
    def __init__(self, network, asset_id=1984, url_ending=""):
        self.method = "GET"
        self.url = f"https://{network}-api.statescan.io/assets/{asset_id}"
        self.url += url_ending

    def extract(self, **kwargs):
        data = extract(self.method, self.url, **kwargs)

        return data
