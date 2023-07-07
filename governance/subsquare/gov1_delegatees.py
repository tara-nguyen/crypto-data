import pandas as pd
from governance.sources.opensquare import Extractor, Transformer


def get_data(chain):
    fields = ["account", "delegatorsCount", "delegatedCapital",
              "delegatedVotes"]
    token_cols = ["delegatedCapital", "delegatedVotes"]

    data = Extractor("subsquare", chain, "/democracy/delegatee").extract()
    df = Transformer(data).transform(fields, token_cols, chain)

    return df


if __name__ == "__main__":
    delegatees = get_data("kusama")
    print(pd.concat([delegatees.head(10), delegatees.tail(10)]).to_string())
