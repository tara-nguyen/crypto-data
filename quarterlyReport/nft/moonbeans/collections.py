from quarterlyReport.nft.sources.moonbeans import MoonbeansExtractor
from quarterlyReport.nft.sources.moonbeans import MoonbeansTransformer


def get_data():
    """Retrieve collection data from Moonbeans and return a dataframe.
    """
    data = MoonbeansExtractor(False).extract()
    df = MoonbeansTransformer(data).to_frame()
    df = df.reindex(columns=["contractAddress", "chain"])

    return df


if __name__ == "__main__":
    collections = get_data()
    print(collections)
