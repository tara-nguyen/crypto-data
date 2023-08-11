from polkaholic import xcm_messages as ms, xcm_transfers as tf
from subscan import fees
from reports.quarterly_etl import print_long_df


def main():
    df_transfers = tf.get_data("polkaholic/data_raw/xcm_transfers_raw.csv")
    for title, key in zip(["XCM Transfers", "XCM Channels"], df_transfers):
        print()
        print(title)
        print(df_transfers[key])

    df_messages = ms.get_data("polkaholic/data_raw/xcm_messages_raw.csv")
    for title, key in zip(
            ["Summary of XCM Messages", "XCM Message Distribution"],
            df_messages):
        print()
        print(title)
        print(df_messages[key])

    df_fees = fees.get_data()
    print()
    print("Chains by Total Chain Fees")
    print_long_df(df_fees)


if __name__ == "__main__":
    main()
