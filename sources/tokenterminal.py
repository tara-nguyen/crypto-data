import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract
from sources.tokenterminal_authorization import bearer


class TokenTerminalExtractor:
    """Extract data from Token Terminal API."""

    def __init__(self, metric):
        self.method = "GET"
        self.url = f"https://api.tokenterminal.com/v2/internal/metrics/{metric}"
        self.headers = {"Content-Type": "application/json",
                        "Authorization": bearer}

    def extract(self, project="polkadot", interval="max"):
        params = {"project_ids": project, "interval": interval}
        data = extract(self.method, self.url, headers=self.headers,
                       params=params)["data"]

        return data


class TokenTerminalTransformer:
    """Convert json-encoded content to a dataframe."""

    def __init__(self, data):
        self.data = data
        self.start = pd.Timestamp(2023, 1, 1)

    def to_frame(self, start=None, end=QuarterlyReport().end_time):
        """
        Keyword arguments:
            start: start point of the time range of interest
            end: end point of the time range of interest
        """
        if start is None:
            start = self.start
        start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]
        df = pd.DataFrame(self.data).query("@start <= timestamp <= @end")
        df["date"] = df["timestamp"].str.slice(stop=10)
        df = df.reindex(columns=["date", "value"])

        return df
