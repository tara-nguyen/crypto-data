from polkaholic import xcm_messages as ms, xcm_transfers as tf
from subscan import fees, token_holder_counts as thc
from reports.quarterly_etl import print_and_load as pl


def main():
    # df_holders = thc.get_data()
    # pl(f"Chains by Token Holder Count", df_holders, "token_holders")

    df_transfers = tf.get_data("polkaholic/data_raw/xcm_transfers_raw.csv")
    pl("XCM Transfers", df_transfers["transfers"], "xcm_transfers")
    pl("XCM Channels", df_transfers["channels"], "xcm_channels")

    df_messages = ms.get_data("polkaholic/data_raw/xcm_messages_raw.csv")
    pl("Summary of XCM Messages", df_messages["summary"],
       "xcm_messages_summary")
    pl("XCM Message Distribution", df_messages["distribution"], "xcm_v3")

    df_fees = fees.get_data()
    pl("Chains by Total Chain Fees", df_fees, "chain_fees")


if __name__ == "__main__":
    main()
