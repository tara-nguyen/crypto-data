import pandas as pd
from reports.quarterly_etl import QuarterlyReport, to_epoch, convert_timestamp
from parachain.sources.coingecko import (CoingeckoExtractor,
                                         CoingeckoTransformer, CoingeckoCoins)
from parachain.coingecko import coin_ids
from time import perf_counter_ns


def get_data(coins=coin_ids.get_data(), start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    """Retrieve price data from CoinGecko and return a dataframe."""
    start, end = [to_epoch(t) for t in [start, end]]
    params = {"vs_currency": "usd", "from": start, "to": end}
    metric = "prices"

    if isinstance(coins, list):
        df = pd.DataFrame([])
        for coin_id in coins:
            data = CoingeckoExtractor(
                f"/{coin_id}/market_chart/range").extract(params)
            df_coin = CoingeckoTransformer(data).to_frame(metric)
            df_coin["id"] = coin_id
            df = pd.concat([df, df_coin])
    elif isinstance(coins, str):
        data = CoingeckoExtractor(
            f"/{coins}/market_chart/range").extract(params)
        df = CoingeckoTransformer(data).to_frame(metric)
    else:
        raise Exception("Invalid data type for the first argument")

    df["date"] = pd.to_datetime(df["timestamp"],
                                unit="ms").dt.strftime("%Y-%m-%d")
    if isinstance(coins, list):
        df = df.merge(CoingeckoCoins().coins)
        df = df.reindex(columns=["chain", "date", "prices"])
    elif isinstance(coins, str):
        df = df.reindex(columns=["date", "prices"])

    return df


if __name__ == "__main__":
    prices = get_data(["polkadot", "kusama"])
    print(prices.sort_values(["chain", "date"]))
