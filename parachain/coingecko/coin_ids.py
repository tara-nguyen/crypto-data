import pandas as pd
from parachain.sources.coingecko import CoingeckoExtractor, CoingeckoCoins


def get_data():
    """Retrieve list of coins from CoinGecko and return a dataframe."""
    ids = CoingeckoCoins().coins["id"]
    df = pd.DataFrame(CoingeckoExtractor("/list").extract())
    ids = df.query("id in @ids")["id"].tolist()

    return ids


if __name__ == "__main__":
    coins = get_data()
    print(coins)
