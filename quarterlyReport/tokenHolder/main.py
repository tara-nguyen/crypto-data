import pandas as pd
from reports.quarterly_etl import QuarterlyReport, print_long_df


def main():
    """Retrieve data on token holder distribution."""
    df = pd.read_csv("holder_distributions.csv")
    for network in QuarterlyReport().networks:
        df_network = df.query("network == @network").drop(columns="network")
        df_network = df_network.eval(
            """tokenPercent = tokenAmount / tokenAmount.sum()
            holderPercent = holderCount / holderCount.sum()
            userToken = tokenAmount - systemToken
            userTokenPercent = userToken / userToken.sum()
            """)
        df_network = df_network.reindex(
            columns=["date", "type", "tokenAmount", "tokenPercent",
                     "holderCount", "holderPercent", "userToken",
                     "userTokenPercent"])
        print(f"\nToken Holder Distribution on {network.title()}")
        print_long_df(df_network)


if __name__ == "__main__":
    main()
