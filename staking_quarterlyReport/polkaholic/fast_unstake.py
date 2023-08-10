import pandas as pd
from staking_quarterlyReport.sources.polkaholic import *
from string import Template


def get_raw_data(path):
    """Retrieve fast-unstake data from Polkaholic's Big Query dataset, save the
    dataset to a csv file, and return a dataframe.
    """
    query = Template("""
    WITH fast_unstake AS (
      SELECT 
        CAST(DATE(ex.block_time) AS STRING) date,
        JSON_VALUE(data[0]) staker,
        CAST(JSON_VALUE(data[1]) AS FLOAT64) / 1e10 amount
      FROM substrate-etl.crypto_polkadot.events0 ev
      JOIN (SELECT * FROM substrate-etl.crypto_polkadot.extrinsics0) ex
      ON ev.extrinsic_id = ex.extrinsic_id
      WHERE
        ex.block_time < "$end"
        AND ex.section = "fastUnstake"
        AND ev.method = "Unbonded"
      ORDER BY 1 DESC
    )
    SELECT
      date,
      COUNT(DISTINCT(staker)) uniqueUsers,
      SUM(amount) amount
    FROM fast_unstake
    GROUP BY date
    ORDER BY 1 DESC
    """)
    data = PolkaholicExtractor().extract(query)
    df = PolkaholicTransformer(data).to_frame()
    df.to_csv(path, index=False)

    return df


def get_data(path="data_raw/fast_unstake_raw.csv"):
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        df = get_raw_data(path)

    df = df.eval("amountPerUser = amount / uniqueUsers")

    # Make all dates appear in the dataframe and fill missing values with zeros
    df["date"] = pd.to_datetime(df["date"])
    index = pd.date_range(df["date"].max(), df["date"].min(), freq="-1D",
                          name="date")
    df = df.set_index("date").reindex(index=index, fill_value=0).reset_index()
    df["date"] = df["date"].astype(str)

    return df


if __name__ == "__main__":
    unstake = get_data()
    print(unstake)
