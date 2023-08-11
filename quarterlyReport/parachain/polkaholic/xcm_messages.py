import pandas as pd
from quarterlyReport.parachain.polkaholic import chains
from quarterlyReport.parachain.sources.polkaholic import PolkaholicExtractor
from quarterlyReport.parachain.sources.polkaholic import PolkaholicTransformer
from string import Template


def get_raw_data(path):
    """Retrieve data on XCM messages from Polkaholic's dataset on Google Big
    Query, save the dataset to a csv file, and return a dataframe.
    """
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
          WHERE origination_ts < "$end"
          AND origination_para_id != 0
          AND destination_para_id != 0
          ORDER BY 2 DESC
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
            version
          FROM `substrate-etl.crypto_kusama.xcm`
          WHERE origination_ts < "$end"
          AND origination_para_id != 0
          AND destination_para_id != 0
          ORDER BY 2 DESC
        )
        SELECT *
        FROM polkadot_xcm
        UNION ALL
        SELECT *
        FROM kusama_xcm
        ORDER BY 2 DESC
        """)
    data = PolkaholicExtractor().extract(query, end=pd.Timestamp(2023, 7, 30))
    df = PolkaholicTransformer(data).to_frame()
    df.to_csv(path, index=False)

    return df


def get_data(path="data_raw/xcm_messages_raw.csv"):
    df_chains = chains.get_data()

    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        df = get_raw_data(path)

    df["date"] = df["timestamp"].str.slice(stop=10)
    df["version"] = df["version"].map(lambda x: "Pre-v3" if x != "v3" else x)
    df = df.merge(df_chains, "left", left_on="origin", right_on="chainID")
    df = df.merge(df_chains, "left", left_on="destination", right_on="chainID")
    df = df.reindex(columns=["relayChain", "date", "timestamp", "chainName_x",
                             "chainName_y", "version"])
    df = df.rename(columns={"chainName_x": "origin",
                            "chainName_y": "destination"})
    df = df.eval("channel = origin.str.cat(destination, ' -> ')")

    df_summary = df.groupby(["version", "relayChain"]).agg(
        {"channel": lambda x: x.nunique(),
         "date": [len, lambda x: x.nunique()]})
    df_summary = df_summary.set_axis(["channels", "totalMessages", "days"],
                                     axis=1)
    df_summary = df_summary.eval("messagesPerDay = totalMessages / days")
    df_summary.reset_index(inplace=True)

    df_v3 = df.query("version == 'v3'")
    df_in = df_v3.groupby("destination").agg({"origin": len})
    df_out = df_v3.groupby("origin").agg({"destination": len})
    df_dist = df_in.join(df_out, how="outer").fillna(0)
    df_dist = df_dist.eval(
        """messageCount = origin + destination
        messagePercent = messageCount / messageCount.sum()
        """)
    df_dist = df_dist.reindex(columns=["messageCount", "messagePercent"])
    df_dist = df_dist.sort_values("messageCount", ascending=False)
    df_dist.reset_index(names="chain", inplace=True)

    dfs = {"summary": df_summary, "distribution": df_dist}

    return dfs


if __name__ == "__main__":
    messages = get_data()
    for df in messages.values():
        with pd.option_context("display.max_columns", None):
            print()
            print(df)
