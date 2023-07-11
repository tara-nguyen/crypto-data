from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import print_long_df


def get_data(chain):
    fields = ["referendumIndex", "turnout", "electorate", "percentage", "votes",
              "ayes", "nays", "delegationAddresses", "directAddresses",
              "delegationCapital", "delegationVotes"]
    token_cols = ["turnout", "electorate", "votes", "ayes", "nays",
                  "delegationCapital", "delegationVotes"]

    data = Extractor("subsquare", chain,
                     "/democracy/referenda/turnout").extract()
    df = Transformer(data).transform(fields, token_cols, chain)

    return df


if __name__ == "__main__":
    votes = get_data("kusama")
    print_long_df(votes)
