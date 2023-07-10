import governance.print_helpers as pr
from governance.sources.opensquare import Extractor, Transformer


def get_data(chain):
    fields = ["motionIndex", "state", "proposer", "title", "content",
              "createdAt", "onchainData_threshold", "onchainData_voting_ayes",
              "onchainData_voting_nays", "onchainData_state_indexer_blockTime",
              "commentsCount", "polkassemblyCommentsCount", "referendumIndex",
              "onchainData_treasuryBounties", "onchainData_treasuryProposals",
              "onchainData_externalProposals"]
    time_cols = ["onchainData_state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", chain, "/motions").extract()
    df = Transformer(data).transform(fields, time_cols=time_cols)
    df = df.dropna(subset="motionIndex")

    return df


if __name__ == "__main__":
    motions = get_data("kusama")
    # df = pr.trim_long_strings(motions, ["title", "content"])
    # df = pr.trim_account_id_strings(df, ["proposer"])
    # pr.print_long_df(df)

    df = motions.reindex(columns=["onchainData_treasuryBounties",
                                  "onchainData_treasuryProposals",
                                  "onchainData_externalProposals"])
    print(df.to_string())
