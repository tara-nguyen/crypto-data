import pandas as pd
from reports.governance_etl import GovernanceReport
from governance.sources.opensquare import Extractor, Transformer


def get_data():
    fields = ["trackId", "trackName", "statistics_addresses_delegator",
              "statistics_addresses_delegatee", "statistics_votes_capital",
              "statistics_votes_votes"]
    token_cols = ["statistics_votes_capital", "statistics_votes_votes"]

    df = pd.DataFrame([])
    for network in GovernanceReport().networks:
        data = Extractor("subsquare", network, "/referenda/tracks").extract()
        df_chain = Transformer(data).transform(fields, token_cols, network,
                                               sort=False)
        df_chain["network"] = network
        df = pd.concat([df, df_chain], ignore_index=True)
    df = df.reindex(columns=["network"] + fields)

    new_cols = ["network"] + fields[:2]
    new_cols += ["delegatorCount", "delegateeCount", "capital", "votes"]
    df = df.set_axis(new_cols, axis=1)

    return df


if __name__ == "__main__":
    delegation = get_data()
    print(delegation.to_string())