import pandas as pd
from staking import StakingReport, get_time
from staking.sources.subsquid import SubsquidExtractor, SubsquidTransformer
from string import Template
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter_ns


def get_data(start=StakingReport().start_era, end=StakingReport().end_era):
    """Retrieve from Subsquid data on validator stake and return a dataframe."""
    metric = "eraStakers"
    template = Template(
        '{"query": "query MyQuery {$metric(where: {role_eq: Validator, '
        'era: {timestamp_gt: \\"$start\\", timestamp_lte: \\"$end\\"}}) '
        '{id totalBonded selfBonded}}", "operationName": "MyQuery"}')

    extractor = SubsquidExtractor("explorer")
    with ThreadPoolExecutor() as exe:
        futures = [exe.submit(extractor.extract, template, metric, daily=True,
                              start=get_time(t), end=get_time(t + 1))
                   for t in range(start - 1, end)]
    data = [future.result() for future in futures]

    transformer = SubsquidTransformer(data, True).to_frame()
    df = transformer.transform_numeric(["totalBonded", "selfBonded"]).df

    df = df.join(transformer.split_id_string(staker_types=["validator"]))
    df = df.rename(columns={"totalBonded": "totalStake",
                            "selfBonded": "selfStake"})
    df = df.reindex(columns=["date", "validator", "totalStake", "selfStake"])

    return df


if __name__ == "__main__":
    print(pd.Timestamp.now())
    t0 = perf_counter_ns()
    history = get_data(StakingReport().end_era-2)
    t1 = perf_counter_ns()
    print(f"Run time: {(t1 - t0) / 1e9 / 60:.2f} minutes")
    # history = StakeChangesByDate(history, ["totalStake", "selfStake"]).get_data(
    #     ["2023-02-28", "2023-03-01"])
    # history = history.sort_values("selfStake_2023-02-28")
    with pd.option_context("display.max_columns", None):
        print(history)
