from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["account", "trackIdCount", "trackIds", "capital", "votes"]
    token_cols = ["capital", "votes"]

    data = Extractor("subsquare", chain, "/referenda/delegatee").extract()
    df = Transformer(data).transform(fields, token_cols, chain)

    return df


if __name__ == "__main__":
    delegatees = get_data("kusama")
    print_long_df(delegatees)
