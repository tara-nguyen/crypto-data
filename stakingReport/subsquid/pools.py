from stakingReport.sources.subsquid import (SubsquidExtractor,
                                            SubsquidTransformer)
from string import Template


def get_data():
    """Retrieve pool data from Subsquid and return a dataframe."""
    metric = "hourlyCharts"
    template = Template(
        '{"query": "query MyQuery {$metric(where: {timestamp_gte:\\"$start\\", '
        'timestamp_lt:\\"$end\\"}) {timestamp nominationPoolsMembersAmount '
        'nominationPoolsPoolsActiveTotalStake '
        'nominationPoolsPoolsInactiveTotalStake '
        'nominationPoolsPoolsActiveAmount '
        'nominationPoolsPoolsInactiveAmount }}", "operationName": "MyQuery"}')

    extractor = SubsquidExtractor("gs_stats")
    data = extractor.extract(template, metric)

    transformer = SubsquidTransformer(data).to_frame().get_daily_data()
    transformer.transform_numeric(["nominationPoolsPoolsActiveTotalStake",
                                   "nominationPoolsPoolsInactiveTotalStake"])
    df = transformer.df.set_axis(["date", "members", "openStake",
                                  "notOpenStake", "openPools", "notOpenPools"],
                                 axis=1)

    df = df.eval(
        """totalPools = openPools + notOpenPools
        totalStake = openStake + notOpenStake
        """)
    df = df.reindex(columns=["date", "members", "totalStake", "totalPools",
                             "openPools"])

    return df


if __name__ == "__main__":
    pools = get_data()
    # print(pools)
    pools = pools.query("date > '2023-05-15'").astype(str)
    print(pools.to_string())
