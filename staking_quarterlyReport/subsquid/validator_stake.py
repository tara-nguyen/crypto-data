from staking_quarterlyReport.sources.subsquid import (SubsquidExtractor,
                                                      SubsquidTransformer)
from string import Template


def get_data():
    """Retrieve from Subsquid data on validator stake and return a dataframe."""
    template = Template(
        '{"query": "{$metric(where: {era: {timestamp_gte: \\"$start\\", '
        'timestamp_lt: \\"$end\\"}, role_eq: Validator}) {era {timestamp} '
        'id selfBonded}}"}')

    data = SubsquidExtractor().extract(template, "eraStakers")
    df = SubsquidTransformer(data).to_frame("selfBonded", ["validator"])

    return df


if __name__ == "__main__":
    stake = get_data()
    print(stake.head().to_string())
