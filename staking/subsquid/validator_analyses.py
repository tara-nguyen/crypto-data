import staking.subsquid.validator_stake as vs
import staking.subsquid.multi_v_single as ms
import staking.subsquid.nominations as nom
import staking.parity.validator_commission as vc
import pandas as pd
from reports.staking_etl import StakingReport, get_era
from staking.sources.subsquid import SubsquidSnapshot, StakeChangesByDate
from concurrent.futures import ThreadPoolExecutor
from functools import reduce
from time import perf_counter_ns


def get_data(start=StakingReport().start_era, end=StakingReport().end_era):
    """Combine results of three different module functions and return a
    dictionary containing seven dataframes.
    """
    with ThreadPoolExecutor() as exe:
        futures = [exe.submit(module.get_data, start=start, end=end)
                   for module in [vs, ms, nom, vc]]
    df_stake, df_multi_v_single, df_nominations, df_commission = [
        future.result() for future in futures]

    # ---------- TOTAL STAKE DISTRIBUTION ----------
    # ----- Daily distributions -----
    df_stake_daily = df_stake.query("date > date.min()")
    df_stake_daily = df_stake_daily.groupby("date").agg(
        {"totalStake": [sum, lambda x: x.std(ddof=0), min, len],
         "selfStake": lambda x: x[x == 0].size})
    df_stake_daily = df_stake_daily.droplevel(0, axis=1)
    df_stake_daily = df_stake_daily.set_axis(
        ["totalStake", "totalStakeStdev", "minTotalStake", "validatorCount",
         "zeroSelfStakeCount"], axis=1)
    df_stake_daily = df_stake_daily.eval(
        "zeroSelfStakeRatio = zeroSelfStakeCount / validatorCount")
    df_stake_daily = df_stake_daily.drop(columns="validatorCount")
    df_stake_daily = df_stake_daily.reset_index().merge(df_multi_v_single)
    df_stake_daily = df_stake_daily.sort_values("date", ascending=False)

    # ----- Snapshots -----
    dates = ["2023-05-03", "2023-05-04", df_stake["date"].max()]
    df_stake_snaps = df_stake.query("date in @dates").copy()
    snapshot = SubsquidSnapshot(df_stake_snaps, "totalStake")
    # snapshot.plot()
    df_stake_snaps["totalStake_MDOT"] = snapshot.get_histogram_bins(3)
    df_stake_snaps = df_stake_snaps.groupby("date")["totalStake_MDOT"]
    df_stake_snaps = df_stake_snaps.value_counts().unstack(0).reset_index()
    # with(pd.option_context("display.max_columns", None)):
    #     print(df_stake_snaps)

    # ---------- VALIDATOR ROTATIONS ----------
    # ----- Validator set changes -----
    df_val_set = df_stake.copy().sort_values("date")

    # Total stake changes, self stake changes, and active validator counts
    df1 = df_val_set.groupby("date").agg({"totalStake": sum, "selfStake": sum,
                                          "validator": len})
    df1 = df1.agg({"totalStake": lambda x: x.diff(),
                   "selfStake": lambda x: x.diff(), "validator": lambda x: x})
    df1 = df1.set_axis(["totalStakeChange", "selfStakeChange",
                        "activeValidators"], axis=1)

    # Cumulative counts of unique validators
    df2 = df_val_set.query("date > date.min()").groupby("validator").head(1)
    df2 = df2.groupby("date")["validator"].count()
    df2 = df2.reindex(index=df1.index).fillna(0)
    df2 = df2.cumsum().rename("cumulativeUniqueValidators")

    # Stake changes from one day to the next
    current_stake = df_val_set.pivot(index="validator", columns="date",
                                     values="totalStake").stack(dropna=False)
    previous_stake = current_stake.groupby("validator").shift()
    df_val_set = current_stake.fillna(0).groupby("validator").diff()
    df_val_set = pd.concat([current_stake, previous_stake, df_val_set], axis=1)
    df_val_set = df_val_set.set_axis(["currentStake", "previousStake",
                                      "stakeChange"], axis=1)

    # Counts of new active validators
    df3 = df_val_set.query("previousStake.isna() and stakeChange > 0")
    df3 = df3.groupby("date")["stakeChange"].count()
    df3 = df3.reindex(index=df1.index).fillna(0).rename("newActiveValidators")

    # Counts of validators with changes versus with no changes in total stake
    # since the previous day
    df4 = df_val_set.groupby("date").agg(
        {"stakeChange": [lambda x: sum(x > 0), lambda x: sum(x < 0)]})
    df4 = df4.set_axis(["stakeGainValidators", "stakeDropValidators"], axis=1)
    df4["stakeUnchangedValidators"] = df_val_set.query(
        "currentStake == previousStake").groupby("date")["stakeChange"].count()

    # Validator set changes - final dataframe
    df_val_set = pd.concat([df1, df2, df3, df4], axis=1)
    df_val_set = df_val_set.query("date > date.min()")
    df_val_set = df_val_set.sort_index(ascending=False).reset_index()

    # ----- Stake changes in specific eras -----
    df_stake_changes = StakeChangesByDate(df_stake, ["totalStake", "selfStake"])
    # Eras 925-926
    # df_925to926 = df_stake_changes.get_data(["2022-12-13", "2022-12-14"])
    # print(df_925to926.to_string())
    # Eras 1020-1021
    dates = ["2023-03-18", "2023-03-19"]
    if get_era(min(dates)) >= start and get_era(max(dates)) <= end:
        df_1020to1021 = df_stake_changes.get_data(dates)
    else:
        df_1020to1021 = None
    # Eras 1066-1067
    dates = ["2023-05-03", "2023-05-04"]
    if get_era(min(dates)) >= start and get_era(max(dates)) <= end:
        df_1066to1067 = df_stake_changes.get_data(dates)
    else:
        df_1066to1067 = None
    # Eras 1086-1087
    # df_1086to1087 = df_stake_changes.get_data(["2023-05-23", "2023-05-24"])
    # print(df_1086to1087.to_string())

    # ---------- NOMINATOR DISTRIBUTION ----------
    # ----- Oversubscribed validators -----
    df_nominators = df_nominations.groupby(["date", "validator"])
    df_nominators = df_nominators.agg({"nominatorStake": sum, "nominator": len})
    df_oversub = df_nominators["nominator"].reset_index(0)

    max_rewardable = df_oversub["date"].transform(
        lambda d: 256 if pd.Timestamp(d) < pd.Timestamp(2023, 1, 14) else 512)
    df_oversub["oversubscribed"] = df_oversub["nominator"] > max_rewardable
    df_oversub["over512"] = (df_oversub["nominator"] > 512).astype(int)
    df_oversub["btwn257and512"] = (
            (df_oversub["nominator"] > 256).astype(int) - df_oversub["over512"])

    df_oversub = df_oversub.drop(columns="nominator").groupby("date").sum()
    df_oversub = df_oversub.sort_index(ascending=False).reset_index()

    # ----- Nominators' preferences -----
    dates = ["2023-05-03", "2023-05-04", df_stake["date"].max()]
    # Validator stake
    df_validator_snaps = df_stake.query("date in @dates")
    df_validator_snaps = df_validator_snaps.set_index(["date", "validator"])

    # Validator commission
    df_validator_prefs = df_commission.query("date in @dates")
    df_validator_prefs = df_validator_prefs.set_index(["date", "validator"])
    df_validator_prefs = df_validator_prefs.groupby(["date", "validator"])
    df_validator_prefs = df_validator_prefs.head(1)

    # Nominator stake and nominator count
    df_nominator_prefs = df_nominators.query("date in @dates")
    df_nominator_prefs = reduce(
        lambda left, right: left.join(right),
        [df_nominator_prefs, df_validator_snaps, df_validator_prefs])
    df_nominator_prefs = df_nominator_prefs.drop(columns="totalStake")
    df_nominator_prefs = df_nominator_prefs.rename(
        columns={"nominator": "nominatorCount", "selfStake": "validatorStake",
                 "commission": "validatorCommission"})
    df_nominator_prefs.reset_index(0, inplace=True)

    dfs = {"stake_daily": df_stake_daily, "stake_snapshots": df_stake_snaps,
           "validator_set": df_val_set, "stake_1020to1021": df_1020to1021,
           "stake_1066to1067": df_1066to1067, "oversubscribed": df_oversub,
           "nominator_preferences": df_nominator_prefs}

    return dfs


if __name__ == "__main__":
    print(pd.Timestamp.now())
    t0 = perf_counter_ns()
    analyses = get_data(1065)
    t1 = perf_counter_ns()
    print(f"Run time: {(t1 - t0) / 1e9 / 60:.2f} minutes")
    for df in analyses.values():
        if df is not None:
            with pd.option_context("display.max_columns", None):
                print()
                # print(df)
                print(df.astype(str).to_string())
            print()
            print_next = input("Print next df? ")
            if print_next == "n":
                break
