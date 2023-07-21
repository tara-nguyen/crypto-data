import staking_stakingReport.polkaholic.rewarded_validators as rv
import staking_stakingReport.subscan.pools as p
import pandas as pd
from staking_stakingReport.sources.polkaholic import PolkaholicExtractor
from string import Template
from time import perf_counter_ns


def get_data():
    """Retrieve data on staking rewards from Polkaholic's dataset on Google Big
    Query and return a dictionary containing four dataframes.
    """
    query = Template("""
    SELECT 
      CAST(DATE(ex.block_time) AS STRING) date,
      JSON_VALUE(data[0]) staker,
      CAST(JSON_VALUE(data[1]) AS FLOAT64) / 1e10 reward
    FROM substrate-etl.crypto_polkadot.events0 ev
    JOIN (SELECT * FROM substrate-etl.crypto_polkadot.extrinsics0) ex
    ON ev.extrinsic_id = ex.extrinsic_id
    WHERE
      ex.block_time >= "$start"
      AND ex.block_time < "$end"
      AND ev.section = "staking"
      AND ev.method = "Rewarded"
    ORDER BY 1 DESC
    """)
    df = PolkaholicExtractor().extract(query)

    # Total rewards by date
    df_total = df.groupby("date").sum(True)
    df_total = df_total.sort_index(ascending=False).reset_index()

    # Nominator rewards
    df_nom = df.query("staker not in @rv.get_data()").drop(columns="staker")
    df_nom = df_nom.groupby("date").sum().reset_index()

    # Pool rewards
    df_pool_list = p.get_data().reindex(columns=["pool", "account"])
    df_pools = df.query("staker in @df_pool_list['account']")
    df_pools = df_pools.merge(df_pool_list, left_on="staker",
                              right_on="account")
    df_pools = df_pools.reindex(columns=["pool", "date", "reward"])
    df_pool_daily_totals = df_pools.groupby("date").sum(True).reset_index()

    # Daily rewards by nominator type: pools versus individuals
    df_nom_type = df_nom.merge(df_pool_daily_totals, "left", on="date")
    df_nom_type = df_nom_type.set_axis(["date", "individual", "pool"], axis=1)
    df_nom_type = df_nom_type.fillna(0).eval(
        """
        individual = individual - pool
        poolShare = pool / (pool + individual)
        """)
    df_nom_type = df_nom_type.sort_values("date", ascending=False)

    # Total rewards by pool
    df_pool_total = df_pools.groupby("pool").sum(True)
    df_pool_total = df_pool_total.set_axis(["totalReward"], axis=1)
    df_pool_total = df_pool_total.sort_values("totalReward", ascending=False)
    df_pool_total = df_pool_total.eval(
        "rewardShare = totalReward / totalReward.sum()")
    df_pool_total.reset_index(inplace=True)

    # Top 5 pools
    top5_pools = df_pool_total.head(5)["pool"]
    df_top5 = df_pools.query("pool in @top5_pools")
    df_top5 = df_top5.groupby(["pool", "date"]).sum(True)
    df_top5 = df_top5.unstack(0, fill_value=0).droplevel(0, axis=1)
    df_top5 = df_top5.reindex(columns=top5_pools).sort_index(ascending=False)
    df_top5.reset_index(inplace=True)

    dfs = {"total": df_total, "nom_type": df_nom_type, "pool": df_pool_total,
           "top5": df_top5}

    return dfs


if __name__ == "__main__":
    print(pd.Timestamp.now())
    t0 = perf_counter_ns()
    rewards = get_data()
    t1 = perf_counter_ns()
    print(f"Run time: {(t1 - t0) / 1e9 / 60:.2f} minutes")
    for df in rewards.values():
        with pd.option_context("display.max_columns", None):
            print()
            # print(df)
            print(df.astype(str).to_string())
