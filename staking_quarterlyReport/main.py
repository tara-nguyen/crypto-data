from polkaholic import fast_unstake as fun, rewards
from subsquid import nominator_prefs as prefs
from reports.quarterly_etl import print_long_df


def main():
    df_fast_unstake = fun.get_data()
    print()
    print("Fast Unstake")
    print_long_df(df_fast_unstake)

    df_rewards = rewards.get_data()
    print()
    print("Rewards by Nominator Type")
    print_long_df(df_rewards)

    df_prefs = prefs.get_data()
    print()
    print("Nominator Preferences")
    print_long_df(df_prefs)


if __name__ == "__main__":
    main()
