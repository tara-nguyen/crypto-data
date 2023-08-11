import pandas as pd
from reports.quarterly_etl import QuarterlyReport, to_epoch
from sources.coingecko import (CoingeckoExtractor, CoingeckoTransformer,
                               CoingeckoCoins)


def get_raw_data(path, coin_ids, start, end):
    """Retrieve price data from CoinGecko, save the data to a csv file, and
    return a dataframe.
    """
    params = {"vs_currency": "usd", "from": start, "to": end}
    metric = "prices"

    df = pd.DataFrame([])
    for cid in coin_ids:
        data = CoingeckoExtractor(f"/{cid}/market_chart/range").extract(params)
        if metric in data:
            df_coin = CoingeckoTransformer(data).to_frame(metric)
            df_coin["id"] = cid
            df = pd.concat([df, df_coin])
        else:
            print(cid, data["error"])
            continue
    df.to_csv(path, index=False)

    return df


def get_data(path="prices_raw.csv", coin_ids=None,
             start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    if coin_ids is None:
        coin_ids = CoingeckoCoins().coin_ids
    elif isinstance(coin_ids, str):
        coin_ids = [coin_ids]
    df_coin_list = pd.DataFrame(CoingeckoCoins().coins_list,
                                columns=["chain", "id"])

    end = end - pd.Timedelta(days=1)
    true_start = start
    if (end - start).days <= 90:
        start = end - pd.Timedelta(days=91)
    start, true_start, end = [to_epoch(t) for t in [start, true_start, end]]

    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        df = get_raw_data(path, CoingeckoCoins().coin_ids, start, end)
    df = df.query("id in @coin_ids")

    if df.size > 0:
        df = df.query("@true_start <= timestamp / 1e3 <= @end").copy()
        df["date"] = pd.to_datetime(df["timestamp"],
                                    unit="ms").dt.strftime("%Y-%m-%d")
        df = df.merge(df_coin_list).reindex(columns=["chain", "date", "prices"])

    return df


if __name__ == "__main__":
    prices = get_data(coin_ids="kusama")
    print(prices)
