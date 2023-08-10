from governance_quarterlyReport.sources.opensquare import *


def get_data(network):
    data = OpensquareExtractor("subsquare", network, "/gov2/tracks").extract()
    df = OpensquareTransformer(data).to_frame(["id", "name"])

    df["name"] = df["name"].str.replace("_", " ").str.title()

    return df


if __name__ == "__main__":
    tracks = get_data("polkadot")
    print(tracks)
