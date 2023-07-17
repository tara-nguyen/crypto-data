import pandas as pd
from reports.quarterly_etl import QuarterlyReport, to_epoch, convert_timestamp
from parachain.sources.coingecko import (CoingeckoExtractor,
                                         CoingeckoTransformer, CoingeckoCoins)
from parachain.coingecko import coin_ids
from time import perf_counter_ns


def get_data(start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    """Retrieve price data from CoinGecko and return a dataframe."""
    start = to_epoch(start)
    end = to_epoch(end)
    params = {"vs_currency": "usd", "from": start, "to": end}

    df = pd.DataFrame([])
    for coin_id in coin_ids.get_data():
        data = CoingeckoExtractor(
            f"/{coin_id}/market_chart/range").extract(params)
        df_coin = CoingeckoTransformer(data).to_frame("prices")
        df_coin["id"] = coin_id
        df = pd.concat([df, df_coin])

    df["date"] = df["timestamp"].map(lambda t: convert_timestamp(t, "%Y-%m-%d",
                                                                 unit="ms"))
    df = df.merge(CoingeckoCoins().coins)
    df = df.reindex(columns=["chain", "date", "prices"])

    return df


if __name__ == "__main__":
    print(pd.Timestamp.now())
    t0 = perf_counter_ns()
    prices = get_data()
    t1 = perf_counter_ns()
    print(f"Run time: {(t1 - t0) / 1e9 / 60:.2f} minutes")
    print(prices)
