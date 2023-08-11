import pandas as pd
from reports.staking_etl import StakingReport, get_time, get_era
from stakingReport.sources.polkadotjs import *


def get_data(start=StakingReport().start_era, end=StakingReport().end_era):
    current_era = get_era(pd.Timestamp.now())
    extractor = PolkadotjsExtractor()
    df = pd.DataFrame([])
    for era in range(max(start, current_era - extractor.history_depth), end):
        data = extractor.extract("Staking", "ErasValidatorPrefs", [era])
        df_era = PolkadotjsTransformer(data).to_frame(
            columns=["validator", "commission"])
        df_era["era"] = era
        df_era["date"] = get_time(era, "%Y-%m-%d")
        df = pd.concat([df, df_era])
    df["commission"] = df["commission"].map(lambda x: x["commission"] / 1e7)
    df = df.reindex(columns=["date", "era", "validator", "commission"])

    return df


def save_to_csv(df, file="validator_commission_on_chain.csv", **kwargs):
    df.to_csv(file, mode="a", index=False, **kwargs)


def main(end=get_era(pd.Timestamp.now())):
    try:
        df = pd.read_csv("validator_commission_on_chain.csv")
    except FileNotFoundError:
        save_to_csv(get_data(end=end))
    else:
        start = df["era"].max() + 1
        if start < end:
            df_new = get_data(start=start, end=end)
            save_to_csv(df_new, header=False)
        else:
            print("Data are up-to-date.")


if __name__ == "__main__":
    commission = get_data()
    with pd.option_context("display.max_columns", None):
        print(commission)
