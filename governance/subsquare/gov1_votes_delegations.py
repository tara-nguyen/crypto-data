import pandas as pd
from governance.sources.opensquare import Extractor, Transformer


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
    df = get_data("kusama")
    print(pd.concat([df.head(10), df.tail(10)]).to_string())
