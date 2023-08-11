import pandas as pd
from reports.quarterly_etl import QuarterlyReport, extract, to_epoch


class DefillamaExtractor:
    """Extract data from DeFiLlama API."""

    def __init__(self, route):
        self.method = "GET"
        self.url = f"https://api.llama.fi{route}"

    def extract(self):
        data = extract(self.method, self.url)

        return data


class DefillamaTransformer:
    """Convert json-encoded content to a dataframe."""

    def __init__(self, data, daily=True):
        self.data = data
        self.daily = daily

    def to_frame(self, start=QuarterlyReport().start_time,
                 end=QuarterlyReport().end_time):
        """
        Keyword arguments:
            start -- start point of the time range of interest
            end -- end point of the time range of interest
        """
        df = pd.DataFrame(self.data)
        if self.daily:
            start, end = [to_epoch(t) for t in [start, end]]

            df = df.query("@start <= date < @end").copy()
            df["date"] = pd.to_datetime(df["date"],
                                        unit="s").dt.strftime("%Y-%m-%d")

        return df


class DefillamaParachains:
    """Store lists of parachains."""

    def __init__(self):
        self.polkadot = ["Moonbeam", "Astar", "Parallel", "Acala", "Interlay",
                         "Equilibrium", "EnergyWeb", "CLV", "Stafi"]
        self.kusama = ["Karura", "Moonriver", "Heiko", "Bifrost", "Kintsugi",
                       "Shiden", "Genshiro"]
        self.chains = self.polkadot + self.kusama
