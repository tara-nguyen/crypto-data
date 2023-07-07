from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["referendumIndex", "trackId", "state_name", "value", "fiatValue",
              "symbolPrice"]
    token_cols = ["value"]

    data = Extractor("dotreasury", chain, "/referenda").extract()
    df = Transformer(data).transform(fields, token_cols, chain)

    return df


if __name__ == "__main__":
    refs = get_data("kusama")
    print_long_df(refs)
