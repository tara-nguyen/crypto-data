from stakingReport.sources.subsquid import (SubsquidExtractor,
                                            SubsquidTransformer)
from string import Template


def get_data():
    """Retrieve staking data from Subsquid and return a dataframe."""
    metric = "hourlyCharts"
    template = Template(
        '{"query": "query MyQuery {$metric(where: {timestamp_gte:\\"$start\\", '
        'timestamp_lt:\\"$end\\"}) {timestamp stakingTotalStake '
        'slotsTokensLockedInParachains stakingInflationRatio '
        'stakingRewardsRatio}}", "operationName": "MyQuery"}')

    extractor = SubsquidExtractor("gs_stats")
    data = extractor.extract(template, metric)

    transformer = SubsquidTransformer(data).to_frame().get_daily_data()
    transformer.transform_numeric(
        ["stakingTotalStake", "slotsTokensLockedInParachains",
         "stakingInflationRatio", "stakingRewardsRatio"],
        [1e10, 1e10, 100, 100])
    df = transformer.df.set_axis(["date", "totalStake", "tokensInParachains",
                                  "inflationRate", "rewardRate"], axis=1)

    return df


if __name__ == "__main__":
    stake = get_data()
    print(stake)
