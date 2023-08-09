from electriccapital import developers as dev
from santiment import dev_activity as da, prices
from tokenterminal import active_developers as ad, code_commits as cc
from reports.quarterly_etl import print_long_df


def main():
    df_prices = prices.get_data("polkadot-new").merge(
        prices.get_data("kusama"), on="date")
    df_prices = df_prices.set_axis(["date", "priceDOT", "priceKSM"], axis=1)
    df_prices_v_activity = df_prices.merge(da.get_data())
    print("Token Prices Vs. Development Activity")
    print_long_df(df_prices_v_activity)

    df_dev_v_commits = ad.get_data().merge(cc.get_data(), on="date")
    print("\nActive Developers Vs. Code Commits")
    print_long_df(df_dev_v_commits)

    df_developers = dev.get_data()
    print("\nDeveloper Distribution")
    print_long_df(df_developers)


if __name__ == "__main__":
    main()
    