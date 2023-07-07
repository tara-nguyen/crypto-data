import pandas as pd
from staking import StakingReport, get_time
from staking.sources.subsquid import SubsquidExtractor, SubsquidTransformer
from string import Template
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter_ns


def get_data(start=StakingReport().start_era, end=StakingReport().end_era):
    """Retrieve staking nomination data from Subsquid and return a dataframe."""
    metric = "eraNominations"
    template = Template(
        '{"query": "query MyQuery {$metric(limit: $limit, offset: $offset, '
        'where: {era: {timestamp_gt: \\"$start\\", timestamp_lte: \\"$end\\"}})'
        '{id vote}}", "operationName": "MyQuery"}')

    extractor = SubsquidExtractor("explorer")
    with ThreadPoolExecutor() as exe:
        futures = [exe.submit(extractor.extract, template, metric, daily=True,
                              start=get_time(t), end=get_time(t + 1))
                   for t in range(start, end)]
    data = [future.result() for future in futures]

    transformer = SubsquidTransformer(data, True).to_frame()
    df = transformer.transform_numeric("vote").df

    df = df.join(transformer.split_id_string(
        staker_types=["validator", "nominator"], drop_eras=False))
    df = df.reindex(columns=["date", "era", "validator", "nominator", "vote"])
    df = df.rename(columns={"vote": "nominatorStake"})

    return df


if __name__ == "__main__":
    print(pd.Timestamp.now())
    t0 = perf_counter_ns()
    nominations = get_data(StakingReport().end_era-1)
    t1 = perf_counter_ns()
    print(f"Run time: {(t1 - t0) / 1e9 / 60:.2f} minutes")
    with pd.option_context("display.max_columns", None):
        print(nominations["nominator"].nunique())
