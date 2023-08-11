import pandas as pd
from reports.staking_etl import extract


class CoingeckoExtractor:
    def __init__(self, path_end="/market_chart/range"):
        self.method = "GET"
        self.url = "https://api.coingecko.com/api/v3/coins/polkadot" + path_end

    def extract(self, querystring):
        """Extract data from CoinGecko."""
        data = extract(self.method, self.url, params=querystring)

        return data


class CoingeckoTransformer:
    def __init__(self, data):
        self.data = data

    def to_frame(self, metric):
        """Convert json-encoded content to a dataframe."""
        df = pd.DataFrame(self.data[metric])
        df = df.set_axis(["timestamp", metric], axis=1)

        return df
