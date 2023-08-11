import pandas as pd
from reports.governance_etl import GovernanceReport
from governanceReport.sources.opensquare import Extractor, Transformer
from governanceReport.print_helpers import trim_long_strings


def get_chain_data(network):
    fields = ["id", "name", "description", "startTime", "latestTime",
              "fiatValue"]
    if network == "polkadot":
        fields += ["fundsValue_polkadot", "fundsCount_polkadot"]
        token_cols = ["fundsValue_polkadot"]
    elif network == "kusama":
        fields += ["fundsValue_kusama", "fundsCount_kusama"]
        token_cols = ["fundsValue_kusama"]
    else:
        raise Exception(f'network must be either "polkadot" or "kusama"')

    time_cols = ["startTime", "latestTime"]

    data = Extractor("dotreasury", network, "/projects_v2").extract()
    df = Transformer(data).transform(fields, token_cols, network, time_cols,
                                     sort_by=token_cols, ascending=False)

    return df


def get_data():
    dfs = [get_chain_data(network) for network in GovernanceReport().networks]
    df = pd.merge(dfs[0], dfs[1], "outer")

    return df


if __name__ == "__main__":
    projects = get_data()
    df = trim_long_strings(projects, ["description"])
    print(df.to_string())
