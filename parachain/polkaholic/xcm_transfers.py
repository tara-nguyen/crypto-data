import pandas as pd
from parachain.sources.polkaholic import (PolkaholicExtractor,
                                          PolkaholicTransformer)
from string import Template


def get_data():
    """Retrieve fast-unstake data from Polkaholic's dataset on Google Big Query
    and return a dataframe.
    """
    query = Template("""
    WITH polkadot_xcm AS (
      SELECT
        'Polkadot' as network,
        origination_ts,
        origination_chain_name,
        destination_chain_name
      FROM `substrate-etl.crypto_polkadot.xcmtransfers`
      WHERE DATE(origination_ts) BETWEEN '2023-04-01' AND '2023-06-30'
      AND destination_execution_status = 'success'
      ORDER BY 1 DESC
    ), kusama_xcm AS (
      SELECT
        'Kusama' as network,
        origination_ts,
        origination_chain_name,
        destination_chain_name
      FROM `substrate-etl.crypto_kusama.xcmtransfers`
      WHERE DATE(origination_ts) BETWEEN '2023-04-01' AND '2023-06-30'
      AND destination_execution_status = 'success'
      ORDER BY 1 DESC
    )
    SELECT *
    FROM polkadot_xcm
    UNION ALL
    SELECT *
    FROM kusama_xcm
    """)
    data = PolkaholicExtractor().extract(query)
    df = PolkaholicTransformer(data).to_frame()

    return df


if __name__ == "__main__":
    xcm = get_data()
    print(xcm)
