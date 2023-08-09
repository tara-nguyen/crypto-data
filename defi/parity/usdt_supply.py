import pandas as pd
from reports.quarterly_etl import QuarterlyReport


def get_data(path="usdt_supply.csv", start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    start, end = [t.strftime("%Y-%m-%d") for t in [start, end]]

    df = pd.read_csv(path).query("@start <= date < @end")
    df = df.replace(["statemint", "statemine"],
                    ["Asset Hub-Polkadot", "Asset Hub-Kusama"])
    df = df.pivot(index="date", columns="chain_name", values="sum_of_usdt")
    df = df.sort_index(ascending=False).reset_index()

    return df


if __name__ == "__main__":
    supply = get_data()
    print(supply)
