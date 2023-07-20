from reports.staking_etl import StakingReport, get_time
from staking_stakingReport.sources.subsquid import SubsquidExtractor, SubsquidTransformer
from string import Template


def get_data(start=StakingReport().start_era, end=StakingReport().end_era):
    """Retrieve from Subsquid data on multi-validator operators versus single-
    validator operators and return a dataframe."""
    metric = "hourlyCharts"
    template = Template(
        '{"query": "query MyQuery {$metric(where: {timestamp_gte:\\"$start\\", '
        'timestamp_lt:\\"$end\\"}) {stakingTotalStakeValidatorsMultiAccount '
        'stakingTotalStakeValidatorsSingleAccount timestamp}}", '
        '"operationName": "MyQuery"}')

    extractor = SubsquidExtractor("gs_stats_new")
    data = extractor.extract(template, metric, start=get_time(start),
                             end=get_time(end))

    transformer = SubsquidTransformer(data).to_frame().get_daily_data()
    transformer.transform_numeric(["stakingTotalStakeValidatorsMultiAccount",
                                   "stakingTotalStakeValidatorsSingleAccount"])
    df = transformer.df.set_axis(["date", "multiAccountStake",
                                  "singleAccountStake"], axis=1)
    df = df.eval("singleAccountShare = singleAccountStake "
                 "/ (singleAccountStake + multiAccountStake)")

    return df


if __name__ == "__main__":
    stake = get_data()
    print(stake)
