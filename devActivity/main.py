from electriccapital import developers as dev
from santiment import dev_activity as da, prices
from tokenterminal import active_developers as ad, code_commits as cc
from reports.quarterly_etl import print_and_load as pl


def main():
    df_prices = prices.get_data("polkadot-new").merge(
        prices.get_data("kusama"), on="date")
    df_prices = df_prices.set_axis(["date", "priceDOT", "priceKSM"], axis=1)
    df_prices_v_activity = df_prices.merge(da.get_data())
    pl("Token Prices Vs. Development Activity", df_prices_v_activity,
       "prices_v_activity")

    df_dev_v_commits = ad.get_data().merge(cc.get_data(), on="date")
    pl("Active Developers Vs. Code Commits", df_dev_v_commits, "dev_v_commits")

    df_developers = dev.get_data()
    pl("Developer Distribution", df_developers, "developers")


if __name__ == "__main__":
    main()
    