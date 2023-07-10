import pandas as pd
from reports.governance_etl import GovernanceReport
from governance.sources.opensquare import Extractor, Transformer


def get_data():
    fields = ["address_delegator", "address_delegatee", "votes_capital",
              "votes_votes"]
    token_cols = ["votes_capital", "votes_votes"]

    df = pd.DataFrame([])
    for chain in GovernanceReport().chains:
        data = Extractor("subsquare", chain, "/democracy/summary").extract()
        df_chain = Transformer(data).transform(fields, token_cols, chain)
        df_chain["chain"] = chain
        df = pd.concat([df, df_chain], ignore_index=True)
    df = df.reindex(columns=["chain"] + fields)

    return df


if __name__ == "__main__":
    delegation = get_data()
    print(delegation.to_string())
