import pandas as pd
import chains
from parachain.sources.polkaholic import (PolkaholicExtractor,
                                          PolkaholicTransformer)
from string import Template


def get_data():
    """Retrieve data on XCM transfers from Polkaholic's dataset on Google Big
    Query and return a dataframe.
    """
    df_chains = chains.get_data()

    query = Template("""
    WITH polkadot_xcm AS (
      SELECT
        'Polkadot' as relayChain,
        CAST(origination_ts AS STRING) timestamp,
        origination_para_id origin,
        destination_para_id destination
      FROM `substrate-etl.crypto_polkadot.xcmtransfers`
      WHERE DATE(origination_ts) >= "$start"
      AND DATE(origination_ts) < "$end"
      AND destination_execution_status = 'success'
    ), kusama_xcm AS (
      SELECT
        'Kusama' as relayChain,
        CAST(origination_ts AS STRING) timestamp,
        origination_para_id origin,
        destination_para_id destination
      FROM `substrate-etl.crypto_kusama.xcmtransfers`
      WHERE DATE(origination_ts) >= "$start"
      AND DATE(origination_ts) < "$end"
      AND destination_execution_status = 'success'
      ORDER BY 2 DESC
    )
    SELECT *
    FROM polkadot_xcm
    UNION ALL
    SELECT *
    FROM kusama_xcm
    ORDER BY 2 DESC
    """)
    data = PolkaholicExtractor().extract(query)
    df = PolkaholicTransformer(data).to_frame()

    df["date"] = df["timestamp"].str.slice(stop=10)
    df = df.merge(df_chains, "left", left_on=["relayChain", "origin"],
                  right_on=["relayChain", "paraID"])
    df = df.merge(df_chains, "left", left_on=["relayChain", "destination"],
                  right_on=["relayChain", "paraID"])
    df = df.reindex(columns=["relayChain", "date", "timestamp", "chainName_x",
                             "chainName_y"])
    df = df.rename(columns={"chainName_x": "origin",
                            "chainName_y": "destination"})

    return df


if __name__ == "__main__":
    transfers = get_data()
    with pd.option_context("display.max_columns", None):
        print(transfers)
