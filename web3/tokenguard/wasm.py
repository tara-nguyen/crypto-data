import pandas as pd
from reports.quarterly_etl import QuarterlyReport


def get_data(network, file_path_prefix="", end=QuarterlyReport().end_time):
    contracts_file_path = file_path_prefix + network + "_created_scs.csv"
    users_file_path = file_path_prefix + network + "_active_users.csv"

    df_contracts = pd.read_csv(contracts_file_path)
    df = df_contracts.merge(pd.read_csv(users_file_path), "outer")

    df["activeUsersCumulative"] = df["Cum Active Users"].map(
        lambda s: int(s.replace(",", "")))
    df = df.reindex(columns=["Daily", "Count", "activeUsersCumulative"])
    df = df.rename(columns={"Daily": "date", "Count": "contracts"})

    index = pd.date_range(df["date"].min(), end.strftime("%Y-%m-%d"), freq="1D",
                          name="date").strftime("%Y-%m-%d")[:-1]
    df = df.set_index("date").reindex(index=index)
    df["contracts"] = df["contracts"].fillna(0).cumsum()
    df["activeUsersCumulative"] = df["activeUsersCumulative"].ffill()
    df = df.sort_index(ascending=False).reset_index()

    return df


if __name__ == "__main__":
    wasm = get_data("alephzero")
    print(wasm.to_string())
