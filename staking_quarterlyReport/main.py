from polkaholic import fast_unstake as fun, rewards
from subsquid import nominator_prefs as prefs
from reports.quarterly_etl import print_long_df


def main():
    path_start = "polkaholic/data_raw/"

    df_fast_unstake = fun.get_data(path_start + "fast_unstake_raw.csv")
    print("Fast Unstake")
    print_long_df(df_fast_unstake)

    df_rewards = rewards.get_data(path_start + "rewards_raw.csv",
                                  path_start + "rewarded_validators_raw.csv")
    print("\nRewards by Nominator Type")
    print_long_df(df_rewards)

    df_prefs = prefs.get_data("subsquid/validator_commission.csv")
    print("\nNominator Preferences")
    print_long_df(df_prefs)


if __name__ == "__main__":
    main()
