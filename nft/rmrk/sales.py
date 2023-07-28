import pandas as pd
from reports.quarterly_etl import QuarterlyReport
from nft.coingecko import prices


def get_data(start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    df_prices = prices.get_data("kusama", start=start, end=end)

    start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]
    df = pd.read_csv("Sales Volume.csv").set_axis(["date", "amount"], axis=1)
    df = df.merge(df_prices.query("chain == 'Kusama'"))
    df = df.eval("volume = amount * prices")
    df = df.reindex(columns=["date", "volume"])

    return df


if __name__ == "__main__":
    sales = get_data()
    print(sales)
