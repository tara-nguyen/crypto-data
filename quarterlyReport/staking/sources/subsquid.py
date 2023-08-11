import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract, get_token_amount


class SubsquidExtractor:
    def __init__(self):
        self.method = "POST"
        self.url = "https://squid.subsquid.io/polkadot-explorer/graphql"
        self.headers = {"Content-Type": "application/json"}

    def extract(self, template, metric, end=QuarterlyReport().end_time):
        """Extract data from Subsquid.

        Keyword arguments:
            start -- start point of the time range of interest
            end -- end point of the time range of interest
        """
        start = (end - pd.Timedelta(days=1))
        start, end = [t.strftime("%Y-%m-%dT%H:%M:%S.%fZ") for t in [start, end]]
        payload = template.substitute(metric=metric, start=start, end=end)
        data = extract(self.method, self.url, headers=self.headers,
                       data=payload)["data"][metric]

        return data


class SubsquidTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self, token_col, staker_types):
        """Convert json-encoded content to a dataframe."""
        df = pd.DataFrame(self.data)
        df[token_col] = df[token_col].map(
            lambda x: get_token_amount(x, "polkadot"))
        df["date"] = df["era"].map(lambda x: x["timestamp"][:10])

        id_parts = df["id"].map(lambda s: s.split("-"))
        df_id = pd.DataFrame(id_parts.tolist(), columns=["era"] + staker_types)

        df = pd.concat([df_id, df.drop(columns=["era", "id"])], axis=1)
        df = df.reindex(columns=["date"] + df.columns[:-1].tolist())

        return df
