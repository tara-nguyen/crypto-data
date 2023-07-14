import pandas as pd
from reports.governance_etl import GovernanceReport
from governance.sources.opensquare import Extractor, Transformer


def get_data():
    fields = ["address_delegator", "address_delegatee", "votes_capital",
              "votes_votes"]
    token_cols = ["votes_capital", "votes_votes"]

    df = pd.DataFrame([])
    for network in GovernanceReport().networks:
        data = Extractor("subsquare", network, "/democracy/summary").extract()
        df_chain = Transformer(data).transform(fields, token_cols, network)
        df_chain["network"] = network
        df = pd.concat([df, df_chain], ignore_index=True)
    df = df.reindex(columns=["network"] + fields)

    return df


if __name__ == "__main__":
    delegation = get_data()
    print(delegation.to_string())
