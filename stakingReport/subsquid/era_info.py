from reports.staking_etl import StakingReport, get_time
from stakingReport.sources.subsquid import (SubsquidExtractor,
                                            SubsquidTransformer)
from string import Template


def get_data(start=StakingReport().start_era, end=StakingReport().end_era):
    """Retrieve supply data from Subsquid and return a dataframe."""
    metric = "eras"
    template = Template(
        '{"query": "query MyQuery {$metric(where: {timestamp_gte:\\"$start\\", '
        'timestamp_lt:\\"$end\\"}) {timestamp index startedAt}}", '
        '"operationName": "MyQuery"}')

    extractor = SubsquidExtractor("explorer")
    data = extractor.extract(template, metric, start=get_time(start),
                             end=get_time(end + 1))

    transformer = SubsquidTransformer(data).to_frame().get_daily_data()
    df = transformer.df.set_axis(["date", "era", "start_block"], axis=1)

    return df


if __name__ == "__main__":
    eras = get_data()
    print(eras)
