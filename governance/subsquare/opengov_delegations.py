import pandas as pd
from governance import GovernanceReport
from governance.sources.opensquare import Extractor, Transformer


def get_data():
    fields = ["trackId", "trackName", "statistics_addresses_delegator",
              "statistics_addresses_delegatee", "statistics_votes_capital",
              "statistics_votes_votes"]
    token_cols = ["statistics_votes_capital", "statistics_votes_votes"]

    df = pd.DataFrame([])
    for chain in GovernanceReport().chains:
        data = Extractor("subsquare", chain, "/referenda/tracks").extract()
        df_chain = Transformer(data).transform(fields, token_cols, chain,
                                               sort=False)
        df_chain["chain"] = chain
        df = pd.concat([df, df_chain], ignore_index=True)
    df = df.reindex(columns=["chain"] + fields)

    return df


if __name__ == "__main__":
    delegation = get_data()
    print(delegation.to_string())