import pandas as pd
from reports.quarterly_etl import print_long_df


def main():
    df = pd.read_csv("holder_distributions.csv")
    for network in ["polkadot", "kusama"]:
        df_network = df.query("network == @network").drop(columns="network")
        df_network = df_network.eval(
            """tokenPercent = tokenAmount / tokenAmount.sum()
            holderPercent = holderCount / holderCount.sum()
            systemTokenPercent = systemToken / tokenAmount
            """)
        df_network = df_network.reindex(
            columns=["date", "type", "tokenAmount", "tokenPercent",
                     "holderCount", "holderPercent", "systemToken",
                     "systemTokenPercent", "systemAccounts"])
        print()
        print(f"Token Holder Distribution on {network.title()}")
        print_long_df(df_network)


if __name__ == "__main__":
    main()
