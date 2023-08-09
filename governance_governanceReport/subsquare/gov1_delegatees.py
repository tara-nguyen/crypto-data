from governance_governanceReport.sources.opensquare import Extractor, Transformer
from governance_governanceReport.print_helpers import print_long_df


def get_data(network):
    fields = ["account", "delegatorsCount", "delegatedCapital",
              "delegatedVotes"]
    token_cols = ["delegatedCapital", "delegatedVotes"]

    data = Extractor("subsquare", network, "/democracy/delegatee").extract()
    df = Transformer(data).transform(fields, token_cols, network)

    return df


if __name__ == "__main__":
    delegatees = get_data("kusama")
    print_long_df(delegatees)
