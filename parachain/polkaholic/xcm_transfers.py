import pandas as pd
import chains
from parachain.sources.polkaholic import (PolkaholicExtractor,
                                          PolkaholicTransformer)
from string import Template


def get_data(start=QuarterlyReport().start_time,
             end=QuarterlyReport().end_time):
    """Retrieve data on XCM transfers from Polkaholic's dataset on Google Big
    Query and return a dataframe.
    """
    df_chains = chains.get_data()

    query = Template(
        """
        WITH polkadot_xcm AS (
          SELECT
            'Polkadot' as relayChain,
            CAST(origination_ts AS STRING) timestamp,
            origination_para_id origin,
            destination_para_id destination,
          FROM `substrate-etl.crypto_polkadot.xcmtransfers`
          WHERE origination_ts >= "$start"
          AND origination_ts < "$end"
          AND destination_execution_status = "success"
        ), kusama_xcm AS (
          SELECT
            'Kusama' as relayChain,
            CAST(origination_ts AS STRING) timestamp,
            CASE
              WHEN origination_para_id = 0 THEN 2
              ELSE origination_para_id + 2e4
            END origin,
            CASE
              WHEN destination_para_id = 0 THEN 2
              ELSE destination_para_id + 2e4
            END destination,
          FROM `substrate-etl.crypto_kusama.xcmtransfers`
          WHERE origination_ts >= "$start"
          AND origination_ts < "$end"
          AND destination_execution_status = "success"
          ORDER BY 2 DESC
        )
        SELECT *
        FROM polkadot_xcm
        UNION ALL
        SELECT *
        FROM kusama_xcm
        ORDER BY 2 DESC
        """)
    data = PolkaholicExtractor().extract(query, start=start, end=end)
    df = PolkaholicTransformer(data).to_frame()

    df = df.merge(df_chains, "left", left_on="origin", right_on="chainID")
    df = df.merge(df_chains, "left", left_on="destination", right_on="chainID")
    df = df.reindex(columns=["relayChain", "chainName_x", "chainName_y"])
    df = df.set_axis(["relayChain", "origin", "destination"], axis=1)

    df_in = df.groupby("destination").agg(
        {"origin": [len, lambda x: x.nunique()]})
    df_out = df.groupby("origin").agg(
        {"destination": [len, lambda x: x.nunique()]})
    df_activities = df_in.join(df_out, how="outer").fillna(0)
    df_activities = df_activities.set_axis(
        ["transfersIn", "uniqueOrigins", "transfersOut", "uniqueDestinations"],
        axis=1)
    df_activities = df_activities.eval(
        "channels = uniqueOrigins + uniqueDestinations")
    df_activities = df_activities.reindex(columns=["transfersIn",
                                                   "transfersOut", "channels"])
    df_activities = df_activities.sort_values(df_activities.columns.tolist(),
                                              ascending=False)
    df_activities.reset_index(names="chain", inplace=True)

    df_channels = df.reindex(columns=["relayChain", "origin", "destination"])
    df_counts = df_channels.eval("channel = origin + destination")
    df_counts = df_counts.groupby("relayChain").agg(
        {"channel": lambda x: x.nunique()})

    df_channels = df_channels.value_counts().reset_index()
    df_channels = df_channels.rename(columns={"count": "transfers"})
    df_channels = df_channels.sort_values(["relayChain", "transfers"],
                                          ascending=False)

    dfs = {"activities": df_activities, "counts": df_counts,
           "channels": df_channels}

    return dfs


if __name__ == "__main__":
    transfers = get_data()
    for df in transfers.values():
        with pd.option_context("display.max_columns", None):
            print()
            print(df)
