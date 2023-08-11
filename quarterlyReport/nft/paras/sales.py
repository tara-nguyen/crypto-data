import pandas as pd
from reports.quarterly_etl import QuarterlyReport, to_epoch
from sources.paras import ParasExtractor, ParasTransformer
from quarterlyReport.nft.coingecko import prices


def get_data(price_file_path="../coingecko/prices_raw.csv",
             start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    df_prices = prices.get_data(price_file_path, "astar", start, end)

    start, end = [to_epoch(t) * 1e3 for t in [start, end]]
    params = {"__sort": "_id::-1", "type": "marketplace_token_bought"}

    data_all = []
    while True:
        data = ParasExtractor().extract(params)
        if len(data) > 0:
            data_all.extend(data)
            if data[-1]["updated_at"] > start:
                params["__next_id"] = data[-1]["_id"]
        else:
            break

    df = ParasTransformer(data_all).to_frame()
    df = df.query("@start <= updated_at < @end").copy()
    df["date"] = pd.to_datetime(df["updated_at"],
                                unit="ms").dt.strftime("%Y-%m-%d")
    df = df.merge(df_prices, on="date")
    df["volume"] = df["content_price"].astype(float) / 1e18 * df["prices"]

    item_count = df.shape[0]
    collection_count = df["content_contract_address"].nunique()
    volume = df["volume"].sum()
    stats = [item_count, collection_count, volume]

    return stats


if __name__ == "__main__":
    sales = get_data()
    print(f"Items: {sales[0]}")
    print(f"Collections: {sales[1]}")
    print(f"Volume: ${round(sales[2], 2)}")
