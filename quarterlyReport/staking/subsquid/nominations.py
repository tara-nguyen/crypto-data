from sources.subsquid import SubsquidExtractor, SubsquidTransformer
from string import Template


def get_data():
    """Retrieve staking nomination data from Subsquid and return a dataframe."""
    template = Template(
        '{"query": "{$metric(where: {era: {timestamp_gte: \\"$start\\", '
        'timestamp_lt: \\"$end\\"}}) {era {timestamp} id vote}}"}')

    data = SubsquidExtractor().extract(template, "eraNominations")
    df = SubsquidTransformer(data).to_frame("vote", ["validator", "nominator"])

    return df


if __name__ == "__main__":
    nominations = get_data()
    print(nominations.head().to_string())
