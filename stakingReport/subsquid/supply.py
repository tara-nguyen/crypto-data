from stakingReport.sources.subsquid import (SubsquidExtractor,
                                            SubsquidTransformer)
from string import Template


def get_data():
    """Retrieve supply data from Subsquid and return a dataframe."""
    metric = "issuances"
    template = Template(
        '{"query": "query MyQuery {$metric(where: {timestamp_gte:\\"$start\\", '
        'timestamp_lt:\\"$end\\"}) {timestamp volume}}", '
        '"operationName": "MyQuery"}')

    extractor = SubsquidExtractor("gs_stats_old")
    data = extractor.extract(template, metric)

    transformer = SubsquidTransformer(data).to_frame()
    transformer.get_daily_data().transform_numeric("volume")
    df = transformer.df.rename(columns={"volume": "supply"})

    return df


if __name__ == "__main__":
    supply = get_data()
    print(supply)
