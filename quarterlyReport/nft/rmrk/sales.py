import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from quarterlyReport.nft.coingecko import prices


def get_data(price_file_path="../coingecko/prices_raw.csv",
             sales_file_path="Sales Volume.csv",
             start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    chain = "kusama"
    df_prices = prices.get_data(price_file_path, chain, start, end)

    df = pd.read_csv(sales_file_path).set_axis(["date", "amount"], axis=1)
    df = df.merge(df_prices.query("chain == 'Kusama'"))
    df = df.eval("totalUSD = amount * prices")
    df["chain"] = chain
    df = df.reindex(columns=["date", "chain", "totalUSD"])

    return df


if __name__ == "__main__":
    sales = get_data()
    print(sales)
