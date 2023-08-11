import governanceReport.print_helpers as pr
from governanceReport.sources.opensquare import Extractor, Transformer


def get_data(network):
    fields = ["index", "parentBountyId", "state", "proposer",
              "onchainData_beneficiary", "title", "content",
              "onchainData_description", "onchainData_value", "createdAt",
              "onchainData_curator", "onchainData_meta_curatorDeposit",
              "onchainData_state_indexer_blockTime", "onchainData_unlockAt",
              "commentsCount", "polkassemblyCommentsCount"]
    token_cols = ["onchainData_value", "onchainData_meta_curatorDeposit"]
    time_cols = ["onchainData_state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", network, "/treasury/child-bounties").extract()
    df = Transformer(data).transform(fields, token_cols, network, time_cols)

    return df


if __name__ == "__main__":
    bounties = get_data("kusama")
    df = pr.trim_long_strings(bounties, ["title", "content",
                                         "onchainData_description"])
    df = pr.trim_account_id_strings(df, ["proposer", "onchainData_beneficiary",
                                         "onchainData_curator"])
    pr.print_long_df(df)
