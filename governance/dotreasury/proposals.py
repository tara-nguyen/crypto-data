from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["proposalIndex", "trackInfo_id", "latestState_state", "value",
              "fiatValue", "symbolPrice"]
    token_cols = ["value"]

    data = Extractor("dotreasury", chain, "/proposals").extract()
    df = Transformer(data).transform(fields, token_cols, chain)

    return df


if __name__ == "__main__":
    proposals = get_data("kusama")
    print_long_df(proposals)
