from governance_governanceReport.sources.opensquare import Extractor, Transformer
from governance_governanceReport.print_helpers import print_long_df


def get_data(network):
    fields = ["proposalIndex", "trackInfo_id", "latestState_state", "value",
              "fiatValue", "symbolPrice"]
    token_cols = ["value"]

    data = Extractor("dotreasury", network, "/proposals").extract()
    df = Transformer(data).transform(fields, token_cols, network)

    return df


if __name__ == "__main__":
    proposals = get_data("kusama")
    print_long_df(proposals)
