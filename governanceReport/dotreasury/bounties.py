from governanceReport.sources.opensquare import Extractor, Transformer
from governanceReport.print_helpers import print_long_df


def get_data(network):
    fields = ["bountyIndex", "state_state", "value", "fiatValue", "symbolPrice"]
    token_cols = ["value"]

    data = Extractor("dotreasury", network, "/bounties").extract()
    df = Transformer(data).transform(fields, token_cols, network)

    return df


if __name__ == "__main__":
    bounties = get_data("kusama")
    print_long_df(bounties)
