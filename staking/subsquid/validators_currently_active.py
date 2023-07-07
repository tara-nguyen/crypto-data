from staking.sources.subsquid import SubsquidExtractor, SubsquidTransformer
from string import Template


def get_data():
    """Retrieve from Subsquid the IDs of currently active validators and return
    a pandas Series.
    """
    metric = "eras"
    template = Template(
        '{"query": "query MyQuery {$metric(limit: 1, orderBy: timestamp_DESC, '
        'where: {timestamp_lt:\\"$end\\"}) {stakers(where: '
        '{role_eq: Validator}) {id}}}", "operationName": "MyQuery"}')

    extractor = SubsquidExtractor("explorer")
    data = extractor.extract(template, metric)

    transformer = SubsquidTransformer(data).to_frame("stakers", nested=True)
    validators = transformer.split_id_string(to_frame=False)[-1]

    return validators


if __name__ == "__main__":
    validators = get_data()
    print(validators)
