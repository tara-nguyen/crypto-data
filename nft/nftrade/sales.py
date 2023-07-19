import pandas as pd
from quarterly_etl import QuarterlyReport
from sources.nftrade import NftradeExtractor, NftradeTransformer
from parachain.coingecko import prices


def get_data(start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]
    data_all = []
    skip = 0
    i = 0
    while True:
        data = NftradeExtractor().extract(skip)
        data_all.extend(data)
        if data[-1]["createdAt"] > start:
            i += 1
            skip = NftradeExtractor().limit * i
        else:
            break
    df = NftradeTransformer(data_all).to_frame()

    df = df.query("@start <= createdAt <= @end and type == 'SOLD'").copy()
    df["date"] = df["createdAt"].str.slice(stop=10)
    df = df.merge(prices.get_data("moonbeam", start=start, end=end))
    df["volume"] = df["price"].astype(float) * df["prices"]
    df = df.reindex(columns=["date", "volume"])

    index = pd.date_range(start, end, freq="1D",
                          name="date").strftime("%Y-%m-%d")[:-1]
    df = df.groupby("date").sum().reindex(index=index, fill_value=0)
    df.reset_index(inplace=True)

    return df


if __name__ == "__main__":
    sales = get_data()
    print(sales.to_string())
