import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract, to_epoch
from functools import reduce


class ElectricCapitalExtractor:
    def __init__(self, metric="dev_mau_by_dev_type", ecosystem="polkadot"):
        self.method = "GET"
        self.url = "https://www.developerreport.com/api/charts/"
        self.url += f"{metric}/{ecosystem}"

    def extract(self):
        data = extract(self.method, self.url)["series"]

        return data


class ElectricCapitalTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self, start=QuarterlyReport().start_time,
                 end=QuarterlyReport().end_time, new_cols=None):
        """Convert json-encoded content to a dataframe."""
        start, end = [to_epoch(t) * 1e3 for t in [start, end]]

        dfs = []
        for d in self.data:
            cols = ["timestamp"] + [d["name"]]
            dfs.append(pd.DataFrame(d["data"], columns=cols))
        df = reduce(lambda l, r: l.merge(r, on="timestamp"), dfs)

        df = df.query("@start <= timestamp <= @end").copy()
        df["date"] = pd.to_datetime(df["timestamp"],
                                    unit="ms").dt.strftime("%Y-%m-%d")
        df = df.reindex(columns=["date"] + df.columns[1:-1].tolist())
        if new_cols is not None:
            df = df.set_axis(["date"] + new_cols, axis=1)
        df = df.sort_values("date", ascending=False)

        return df
