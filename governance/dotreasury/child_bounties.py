from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["index", "parentBountyId", "state_state", "value", "fiatValue",
              "symbolPrice"]
    token_cols = ["value"]

    data = Extractor("dotreasury", chain, "/child-bounties").extract()
    df = Transformer(data).transform(fields, token_cols, chain)

    return df


if __name__ == "__main__":
    bounties = get_data("kusama")
    print_long_df(bounties)