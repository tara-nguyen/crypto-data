from sources.bluez import BluezExtractor, BluezTransformer


def get_data():
    payload = ('{"query": "{contract {ranking(queryCondition: {page: 1, '
               'pageSize: 10, orderBy: total_volume_desc, '
               'where: {chainId: {eq_: 592}}}) {edges {node {contract {name} '
               'totalVolume totalItem holderAmount}}}}}"}')

    data = BluezExtractor().extract(payload)["contract"]["ranking"]["edges"]
    df = BluezTransformer(data).to_frame()
    df = df.set_axis(["name", "volume", "itemCount", "holderCount"], axis=1)

    return df


if __name__ == "__main__":
    collections = get_data()
    print(collections)
