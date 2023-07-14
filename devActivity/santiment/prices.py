from devActivity.sources.santiment import (SantimentExtractor,
                                           SantimentTransformer)


def get_data(slug):
    data = SantimentExtractor().extract("price_usd", slug=slug)
    df = SantimentTransformer(data).to_frame()
    df = df.set_axis(["date", "priceUSD"], axis=1)

    return df


if __name__ == "__main__":
    prices = get_data("polkadot-new")
    print(prices)
