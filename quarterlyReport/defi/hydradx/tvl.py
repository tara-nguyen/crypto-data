import pandas as pd
from reports.quarterly_etl import QuarterlyReport


def get_data(path="hydradx_tvl_volume_q1q2.csv",
             start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]

    df = pd.read_csv(path).query("@start <= date < @end")
    df = df.rename(columns={"tvl_usd": "HydraDX"})
    df = df.reindex(columns=["date", "HydraDX"])

    return df


if __name__ == "__main__":
    tvl = get_data()
    print(tvl)
