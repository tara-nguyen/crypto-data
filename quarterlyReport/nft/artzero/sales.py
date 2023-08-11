from reports.quarterly_etl import QuarterlyReport
from sources.artzero import ArtzeroExtractor, ArtzeroTransformer
from quarterlyReport.nft.artzero import collections
from quarterlyReport.nft.coingecko import prices


def get_data(file_path_prefix="../coingecko/",
             start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    cols = collections.get_data()["nftContractAddress"]
    price_file_path = file_path_prefix + "prices_raw.csv"
    df_prices = prices.get_data(price_file_path, "astar", start, end)
    start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]

    data = []
    for col in cols:
        payload = f"collection_address={col}&limit=30"
        data.extend(ArtzeroExtractor("/getPurchaseEvents").extract(payload))

    df = ArtzeroTransformer(data).to_frame()
    df["date"] = df["updatedTime"].str.slice(stop=10)
    df = df.query("@start <= date < @end")
    df = df.merge(df_prices).eval("volume = price * prices")

    item_count = df.shape[0]
    collection_count = cols.shape[0]
    volume = df["volume"].sum()
    stats = [item_count, collection_count, volume]

    return stats


if __name__ == "__main__":
    sales = get_data()
    print(f"Items: {sales[0]}")
    print(f"Collections: {sales[1]}")
    print(f"Volume: ${round(sales[2], 2)}")
