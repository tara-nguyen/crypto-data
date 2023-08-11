import pandas as pd
from quarterlyReport.nft.coingecko import prices
from reports.quarterly_etl import QuarterlyReport


def get_data(price_file_path="../coingecko/prices_raw.csv",
             sales_file_path="sales.csv",
             start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    df = pd.read_csv(sales_file_path)

    chains = df["chain"].unique().tolist()
    df_prices = prices.get_data(price_file_path, chains, start, end)
    df_prices["chain"] = df_prices["chain"].str.lower()
    df = df.merge(df_prices).eval("totalUSD = token * prices")
    df = df.reindex(columns=["date", "chain", "totalUSD"])

    dates = pd.date_range(start, end, freq="1D").strftime("%Y-%m-%d")[:-1]
    index_names = ["date", "chain"]
    index = pd.MultiIndex.from_product([dates, chains], names=index_names)
    df = df.set_index(index_names).reindex(index=index, fill_value=0)
    df.reset_index(inplace=True)

    return df


if __name__ == "__main__":
    sales = get_data()
    print(sales)
