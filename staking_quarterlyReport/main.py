from polkaholic import fast_unstake as fun, rewards
from subsquid import nominator_prefs as prefs
from reports.quarterly_etl import print_and_load as pl


def main():
    df_fast_unstake = fun.get_data()
    pl("Fast Unstake", df_fast_unstake, "fast_unstake")

    df_rewards = rewards.get_data()
    pl("Rewards by Nominator Type", df_rewards, "rewards")

    df_prefs = prefs.get_data()
    pl("Nominator Preferences", df_prefs, "nominator_prefs")


if __name__ == "__main__":
    main()
