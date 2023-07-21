import pandas as pd
import chains
from parachain.sources.polkaholic import (PolkaholicExtractor,
                                          PolkaholicTransformer)
from string import Template


def get_data():
    """Retrieve data on XCM messages from Polkaholic's dataset on Google Big
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
            version
          FROM `substrate-etl.crypto_polkadot.xcm`
          WHERE origination_ts >= "$start"
          AND origination_ts < "$end"
        ), kusama_xcm AS (
          SELECT
            'Kusama' as relayChain,
            CAST(origination_ts AS STRING) timestamp,
            CASE
              WHEN origination_para_id = 0 THEN 2
              ELSE origination_para_id
            END origin,
            CASE
              WHEN destination_para_id = 0 THEN 2
              ELSE destination_para_id
            END destination,
            version
          FROM `substrate-etl.crypto_kusama.xcm`
          WHERE origination_ts >= "$start"
          AND origination_ts < "$end"
          ORDER BY 2 DESC
        )
        SELECT *
        FROM polkadot_xcm
        UNION ALL
        SELECT *
        FROM kusama_xcm
        ORDER BY 2 DESC
        """)
    data = PolkaholicExtractor().extract(query, start=pd.Timestamp(2023, 1, 1))
    df = PolkaholicTransformer(data).to_frame()

    df["date"] = df["timestamp"].str.slice(stop=10)
    df = df.merge(df_chains, "left", left_on="origin", right_on="chainID")
    df = df.merge(df_chains, "left", left_on="destination", right_on="chainID")
    df = df.reindex(columns=["relayChain", "date", "timestamp", "chainName_x",
                             "chainName_y", "version"])
    df = df.rename(columns={"chainName_x": "origin",
                            "chainName_y": "destination"})
    df = df.eval("channel = origin + destination")

    df_counts = df.groupby(["version", "relayChain"]).agg(
        {"channel": lambda x: x.nunique(),
         "date": [len, lambda x: x.nunique()]})
    df_counts = df_counts.set_axis(["channels", "totalMessages", "days"], axis=1)
    df_counts = df_counts.eval("messagesPerDay = totalMessages / days")
    df_counts.reset_index(inplace=True)

    df_v3 = df.query("version == 'v3'")
    df_ms = df_v3.reindex(columns=["relayChain", "origin", "destination"])
    df_ms = df_ms.value_counts().reset_index()
    df_ms = df_ms.eval("channel = origin.str.cat(destination, ' -> ')")

    total_ms_counts = df_ms.groupby("relayChain")["count"].sum()
    df_ms = df_ms.merge(total_ms_counts, on="relayChain")
    df_ms = df_ms.eval("messagePercent = count_x / count_y")
    df_ms = df_ms.rename(columns={"count_x": "messageCount"})
    df_ms = df_ms.reindex(columns=["relayChain", "channel", "messageCount",
                                   "messagePercent"])
    df_ms = df_ms.sort_values(["relayChain", "messageCount"], ascending=False)

    dfs = {"summary": df_counts, "message_stats": df_ms}

    return dfs


if __name__ == "__main__":
    messages = get_data()
    for df in messages.values():
        with pd.option_context("display.max_columns", None):
            print()
            print(df)
