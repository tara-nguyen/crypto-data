import governanceReport.print_helpers as pr
from governanceReport.sources.opensquare import Extractor, Transformer


def get_data(network):
    fields = ["referendumIndex", "state", "proposer", "title", "createdAt",
              "onchainData_meta_delay", "onchainData_state_indexer_blockTime"]
    time_cols = ["onchainData_state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", network, "/democracy/referendums").extract()
    df = Transformer(data).transform(fields, time_cols=time_cols)

    return df


if __name__ == "__main__":
    proposals = get_data("kusama")
    df = pr.trim_long_strings(proposals, ["title"])
    df = pr.trim_account_id_strings(df, ["proposer"])
    pr.print_long_df(df)
