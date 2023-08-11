import stakingReport.subsquid.stake_inflation as si
from stakingReport.subsquid import supply
from concurrent.futures import ThreadPoolExecutor


def get_data():
    """Combine results of two different module functions and return a dictionary
    containing two dataframes.
    """
    with ThreadPoolExecutor() as exe:
        futures = [exe.submit(module.get_data) for module in [supply, si]]
    df_supply, df_stake_inflation = [future.result() for future in futures]

    # Total stake and tokens in parachains
    df_overall = df_supply.merge(
        df_stake_inflation[["date", "totalStake", "tokensInParachains"]])
    df_overall = df_overall.eval(
        """
        stakingRate = totalStake / supply
        tokensInParachainsRatio = tokensInParachains / supply
        """)
    df_overall = df_overall.reindex(
        columns=["date", "supply", "totalStake", "stakingRate",
                 "tokensInParachains", "tokensInParachainsRatio"])

    # Staking rate and inflation rate
    df_rates = df_overall[["date", "stakingRate"]].merge(
        df_stake_inflation[["date", "inflationRate"]])
    df_rates = df_rates.eval("rewardRate = inflationRate / stakingRate")

    dfs = {"overall": df_overall, "rates": df_rates}

    return dfs


if __name__ == "__main__":
    staking = get_data()
    for df in staking.values():
        print()
        print(df.to_string())
