from quarterlyReport.devActivity.sources.santiment import SantimentExtractor
from quarterlyReport.devActivity.sources.santiment import SantimentTransformer


def get_data(slug):
    data = SantimentExtractor().extract("price_usd", slug=slug)
    df = SantimentTransformer(data).to_frame()

    return df


if __name__ == "__main__":
    prices = get_data("polkadot-new")
    print(prices)