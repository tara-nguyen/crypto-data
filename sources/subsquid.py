import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract, get_token_amount


class SubsquidExtractor:
    """Extract data from Subsquid graphql."""

    def __init__(self):
        self.method = "POST"
        self.url = "https://squid.subsquid.io/polkadot-explorer/graphql"
        self.headers = {"Content-Type": "application/json"}

    def extract(self, template, metric, end=QuarterlyReport().end_time):
        """
        Keyword arguments:
            end: end point of the time range of interest
        """
        start = (end - pd.Timedelta(days=1))
        start, end = [t.strftime("%Y-%m-%dT%H:%M:%S.%fZ") for t in [start, end]]
        payload = template.substitute(metric=metric, start=start, end=end)
        data = extract(self.method, self.url, headers=self.headers,
                       data=payload)["data"][metric]

        return data


class SubsquidTransformer:
    """Convert json-encoded content to a dataframe."""

    def __init__(self, data):
        self.data = data

    def to_frame(self, token_col, staker_types):
        df = pd.DataFrame(self.data)
        df[token_col] = df[token_col].map(
            lambda x: get_token_amount(x, "polkadot"))
        df["date"] = df["era"].map(lambda x: x["timestamp"][:10])

        # An id string consists of three substrings, separated by a dash: era
        # id, validator id, and nominator id
        id_parts = df["id"].map(lambda s: s.split("-"))
        df_id = pd.DataFrame(id_parts.tolist(), columns=["era"] + staker_types)

        df = pd.concat([df_id, df.drop(columns=["era", "id"])], axis=1)
        df = df.reindex(columns=["date"] + df.columns[:-1].tolist())

        return df
