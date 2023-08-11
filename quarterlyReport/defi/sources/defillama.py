import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract, to_epoch


class DefillamaExtractor:
    def __init__(self, route):
        self.method = "GET"
        self.url = f"https://api.llama.fi{route}"

    def extract(self):
        """Extract data from DeFiLlama."""
        data = extract(self.method, self.url)

        return data


class DefillamaTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self):
        """Convert json-encoded content to a dataframe."""
        df = pd.DataFrame(self.data)

        return df


class DefillamaParachains:
    def __init__(self):
        self.polkadot = ["Moonbeam", "Astar", "Parallel", "Acala", "Interlay",
                         "Equilibrium", "EnergyWeb", "CLV", "Stafi"]
        self.kusama = ["Karura", "Moonriver", "Heiko", "Bifrost", "Kintsugi",
                       "Shiden", "Genshiro"]
        self.chains = self.polkadot + self.kusama


def filter_and_convert(df, start=QuarterlyReport().start_time,
                       end=QuarterlyReport().end_time):
    start, end = [to_epoch(t) for t in [start, end]]

    df = df.query("@start <= date < @end").copy()
    df["date"] = pd.to_datetime(df["date"],
                                unit="s").dt.strftime("%Y-%m-%d")

    return df
