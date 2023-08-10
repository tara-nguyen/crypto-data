import pandas as pd
from staking_quarterlyReport.sources.polkaholic import *
from staking_quarterlyReport.polkaholic import rewarded_validators as rv
from staking_quarterlyReport.subscan import pools
from string import Template


def get_raw_data(path):
    """Retrieve data on staking rewards from Polkaholic's Big Query dataset,
    save the dataset to a csv file, and return a dataframe.
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
    data = PolkaholicExtractor().extract(query)
    df = PolkaholicTransformer(data).to_frame()
    df.to_csv(path, index=False)

    return df


def get_data(rewards_path="data_raw/rewards_raw.csv",
             validators_path="data_raw/rewarded_validators_raw.csv"):
    try:
        df = pd.read_csv(rewards_path)
    except FileNotFoundError:
        df = get_raw_data(rewards_path)

    # Nominator rewards
    validators = rv.get_data(validators_path)
    df_nom = df.query("staker not in @validators").drop(columns="staker")
    df_nom = df_nom.groupby("date").sum().reset_index()

    # Pool rewards
    df_pool_list = pools.get_data().reindex(columns=["pool", "account"])
    df_pools = df.query("staker in @df_pool_list['account']")
    df_pools = df_pools.merge(df_pool_list, left_on="staker",
                              right_on="account")
    df_pools = df_pools.reindex(columns=["pool", "date", "reward"])
    df_pool_daily_totals = df_pools.groupby("date").sum(True).reset_index()

    # Daily rewards by nominator type: pools versus individuals
    df = df_nom.merge(df_pool_daily_totals, "left", on="date")
    df = df.set_axis(["date", "individual", "pool"], axis=1)
    df = df.fillna(0).eval(
        """
        individual = individual - pool
        poolShare = pool / (pool + individual)
        """)
    df = df.sort_values("date", ascending=False)

    return df


if __name__ == "__main__":
    rewards = get_data()
    print(rewards)
