from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["account", "delegatee", "balance", "conviction", "votes"]
    token_cols = ["balance", "votes"]

    data = Extractor("subsquare", chain, "/democracy/delegators").extract()
    df = Transformer(data).transform(fields, token_cols, chain)

    return df


if __name__ == "__main__":
    delegators = get_data("kusama")
    print_long_df(delegators)
