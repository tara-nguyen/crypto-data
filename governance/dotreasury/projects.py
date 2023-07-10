import pandas as pd
from reports.governance_etl import GovernanceReport
from governance.sources.opensquare import Extractor, Transformer
from governance.print_helpers import trim_long_strings


def get_chain_data(chain):
    fields = ["id", "name", "description", "startTime", "latestTime",
              "fiatValue"]
    if chain == "polkadot":
        fields += ["fundsValue_polkadot", "fundsCount_polkadot"]
        token_cols = ["fundsValue_polkadot"]
    elif chain == "kusama":
        fields += ["fundsValue_kusama", "fundsCount_kusama"]
        token_cols = ["fundsValue_kusama"]
    else:
        raise Exception(f'chain must be either "polkadot" or "kusama"')

    time_cols = ["startTime", "latestTime"]

    data = Extractor("dotreasury", chain, "/projects_v2").extract()
    df = Transformer(data).transform(fields, token_cols, chain, time_cols,
                                     sort_by=token_cols, ascending=False)

    return df


def get_data():
    dfs = [get_chain_data(chain) for chain in GovernanceReport().chains]
    df = pd.merge(dfs[0], dfs[1], "outer")

    return df


if __name__ == "__main__":
    projects = get_data()
    df = trim_long_strings(projects, ["description"])
    print(df.to_string())
