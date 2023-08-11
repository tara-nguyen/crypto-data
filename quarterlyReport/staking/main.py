from polkaholic import fast_unstake as fun, rewards
from subsquid import nominator_prefs as prefs
from reports.quarterly_etl import print_long_df


def main():
    file_path_prefix = "polkaholic/data_raw/"

    df_fast_unstake = fun.get_data(file_path_prefix)
    print("Fast Unstake")
    print_long_df(df_fast_unstake)

    df_rewards = rewards.get_data(file_path_prefix)
    print("\nRewards by Nominator Type")
    print_long_df(df_rewards)

    df_prefs = prefs.get_data("subsquid/")
    print("\nNominator Preferences")
    print_long_df(df_prefs)


if __name__ == "__main__":
    main()
