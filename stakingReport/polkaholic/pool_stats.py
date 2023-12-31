from stakingReport.sources.polkaholic import PolkaholicExtractor
from stakingReport.subscan import pools
from string import Template


def get_data(order_by="stake", direction="DESC"):
    """Retrieve data on pool stake and pool member count from Polkaholic's
    dataset on Google Big Query and return a dataframe.
    """
    query = Template("""
    WITH pool_stake AS (
      SELECT 
        CAST(DATE(ex.block_time) AS STRING) date,
        JSON_VALUE(data[0]) staker,
        CAST(JSON_VALUE(data[1]) AS INT64) poolId,
        CAST(JSON_VALUE(data[2]) AS FLOAT64) / 1e10 amount
      FROM substrate-etl.crypto_polkadot.events0 ev
      JOIN (SELECT * FROM substrate-etl.crypto_polkadot.extrinsics0) ex
      ON ev.extrinsic_id = ex.extrinsic_id
      WHERE
        ex.block_time >= "$start"
        AND ex.block_time < "$end"
        AND ev.section = "nominationPools"
        AND ev.method = "Bonded"
      ORDER BY 1 DESC
    )
    SELECT
      poolId,
      SUM(amount) stake,
      COUNT(DISTINCT(staker)) members
    FROM pool_stake
    GROUP BY 1
    ORDER BY $order_by $direction
    """)
    df = PolkaholicExtractor().extract(query, order_by=order_by,
                                       direction=direction)
    df = df.merge(pools.get_data(), left_on="poolId", right_on="id")
    df = df.reindex(columns=["pool", "stake", "members"])

    return df


if __name__ == "__main__":
    stats = get_data("members")
    print(stats)
