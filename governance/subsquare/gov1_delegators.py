import pandas as pd
from governance.sources.opensquare import Extractor, Transformer


def get_data(chain):
    fields = ["account", "delegatee", "balance", "conviction", "votes"]
    token_cols = ["balance", "votes"]

    data = Extractor("subsquare", chain, "/democracy/delegators").extract()
    df = Transformer(data).transform(fields, token_cols, chain)

    return df


if __name__ == "__main__":
    delegators = get_data("kusama")
    print(pd.concat([delegators.head(10), delegators.tail(10)]).to_string())
