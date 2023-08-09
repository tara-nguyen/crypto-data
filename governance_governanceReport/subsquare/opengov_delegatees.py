from governance_governanceReport.sources.opensquare import Extractor, Transformer
from governance_governanceReport.print_helpers import print_long_df


def get_data(network):
    fields = ["account", "trackIdCount", "trackIds", "capital", "votes"]
    token_cols = ["capital", "votes"]

    data = Extractor("subsquare", network, "/referenda/delegatee").extract()
    df = Transformer(data).transform(fields, token_cols, network)

    return df


if __name__ == "__main__":
    delegatees = get_data("kusama")
    print_long_df(delegatees)
