import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract
from .authorization import token_terminal_bearer


class TokenTerminalExtractor:
    def __init__(self, metric):
        self.method = "GET"
        self.url = f"https://api.tokenterminal.com/v2/internal/metrics/{metric}"
        self.headers = {"Content-Type": "application/json",
                        "Authorization": token_terminal_bearer}

    def extract(self, project="polkadot", interval="180d"):
        params = {"project_ids": project, "interval": interval}
        data = extract(self.method, self.url, headers=self.headers,
                       params=params)["data"]

        return data


class TokenTerminalTransformer:
    def __init__(self, data):
        self.data = data
        self.timestamp_format = "%Y-%m-%d"

    def to_frame(self, start=QuarterlyReport().start_time,
                 end=QuarterlyReport().end_time):
        """Convert json-encoded content to a dataframe."""
        start = start.strftime(self.timestamp_format)
        end = end.strftime(self.timestamp_format)

        df = pd.DataFrame(self.data).query("@start <= timestamp <= @end")
        df["date"] = df["timestamp"].str.removesuffix("T00:00:00.000Z")
        df = df.reindex(columns=["date", "value"])

        return df