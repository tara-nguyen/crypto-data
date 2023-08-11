from governanceReport.sources.opensquare import Extractor, Transformer
from governanceReport.print_helpers import print_long_df


def get_data(network):
    fields = ["account", "delegatee", "balance", "conviction", "votes"]
    token_cols = ["balance", "votes"]

    data = Extractor("subsquare", network, "/democracy/delegators").extract()
    df = Transformer(data).transform(fields, token_cols, network)

    return df


if __name__ == "__main__":
    delegators = get_data("kusama")
    print_long_df(delegators)
