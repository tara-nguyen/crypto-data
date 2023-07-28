import pandas as pd
from parachain.sources.polkaholic import *
from parachain.polkaholic import chains
from string import Template


def get_raw_data(path):
    """Retrieve data on XCM transfers from Polkaholic's dataset on Google Big
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
          FROM `substrate-etl.crypto_polkadot.xcmtransfers`
          WHERE destination_execution_status = "success"
          AND origination_ts < "$end"
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
          FROM `substrate-etl.crypto_kusama.xcmtransfers`
          WHERE destination_execution_status = "success"
          AND origination_ts < "$end"
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
    data = PolkaholicExtractor().extract(query)
    df = PolkaholicTransformer(data).to_frame()
    df.to_csv(path, index=False)

    return df


def get_data(path="data_raw/xcm_transfers_raw.csv"):
    df_chains = chains.get_data()

    # path = getcwd() + path
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        df = get_raw_data(path)

    df = df.merge(df_chains, "left", left_on="origin", right_on="chainID")
    df = df.merge(df_chains, "left", left_on="destination", right_on="chainID")

    df["quarter"] = pd.to_datetime(df["timestamp"]).dt.quarter
    df["period"] = df.apply(
        lambda row: f"Q{row['quarter']}_2023"
        if row["timestamp"] >= "2023-01-01" else "pre2023", axis=1)
    df = df.reindex(columns=["period", "relayChain", "chainName_x",
                             "chainName_y"])
    df = df.rename(columns={"chainName_x": "origin",
                            "chainName_y": "destination"})

    df_in = df.groupby(["destination", "period"]).agg({"origin": len})
    df_in = df_in.unstack().droplevel(0, axis=1)
    df_out = df.groupby(["origin", "period"]).agg({"destination": len})
    df_out = df_out.unstack().droplevel(0, axis=1)
    df_transfers = df_in.join(df_out, how="outer", lsuffix="_in",
                              rsuffix="_out")
    df_transfers = df_transfers.fillna(0).eval(
        """pre2023 = pre2023_in + pre2023_out
        Q1_2023 = Q1_2023_in + Q1_2023_out
        Q2_2023 = Q2_2023_in + Q2_2023_out
        total = pre2023 + Q1_2023 + Q2_2023
        """)
    df_transfers = df_transfers.reindex(columns=["pre2023", "Q1_2023",
                                                 "Q2_2023", "total"])
    df_transfers = df_transfers.sort_values("total", ascending=False)
    df_transfers.reset_index(names="chain", inplace=True)

    df_channels = df.dropna().copy()
    df_channels = df_channels.eval(
        "channel = origin.str.cat(destination, ' -> ')")
    df_channels = df_channels.groupby(["channel", "period"]).agg(
        {"relayChain": len})
    df_channels = df_channels.unstack(fill_value=0).droplevel(0, axis=1)
    df_channels = df_channels.eval("total = pre2023 + Q1_2023 + Q2_2023")
    df_channels = df_channels.reindex(columns=["pre2023", "Q1_2023", "Q2_2023",
                                               "total"])
    df_channels = df_channels.sort_values("total", ascending=False)
    df_channels.reset_index(names="channel", inplace=True)

    dfs = {"transfers": df_transfers, "channels": df_channels}

    return dfs


if __name__ == "__main__":
    transfers = get_data()
    for df in transfers.values():
        with pd.option_context("display.max_columns", None):
            print()
            print(df)
