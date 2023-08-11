from sources.artzero import ArtzeroExtractor, ArtzeroTransformer


def get_data():
    payload = "ignoreNoNFT=false"
    data = ArtzeroExtractor("/getCollections").extract(payload)
    df = ArtzeroTransformer(data).to_frame()
    df = df.reindex(columns=["nftContractAddress", "name"])

    return df


if __name__ == "__main__":
    col = get_data()
    print(col)
