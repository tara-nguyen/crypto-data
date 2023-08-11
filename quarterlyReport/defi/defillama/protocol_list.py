from sources.defillama import (DefillamaExtractor, DefillamaTransformer,
                               DefillamaParachains)


def get_data(category):
    chains = DefillamaParachains().chains

    data = DefillamaExtractor("/protocols").extract()
    df = DefillamaTransformer(data).to_frame()

    df = df.explode("chains")
    if df["category"].eq(category).any():
        df = df.query("category == @category and chains in @chains")
    else:
        raise Exception("Invalid category")

    df = df.reindex(columns=["name", "slug", "chains"])
    df = df.rename(columns={"chains": "chain"})

    return df


if __name__ == "__main__":
    for category in ["Dexes", "Lending", "Liquid Staking"]:
        protocols = get_data(category)
        print()
        print(category)
        print(protocols)
