import governance.print_helpers as pr
from governance.sources.opensquare import Extractor, Transformer


def get_data(network):
    fields = ["motionIndex", "state", "proposer", "title", "content",
              "createdAt", "onchainData_threshold", "onchainData_voting_ayes",
              "onchainData_voting_nays", "onchainData_state_indexer_blockTime",
              "commentsCount", "polkassemblyCommentsCount",
              "onchainData_treasuryBounties", "onchainData_treasuryProposals",
              "onchainData_externalProposals"]
    time_cols = ["onchainData_state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", network, "/motions").extract()
    df = Transformer(data).transform(fields, time_cols=time_cols)
    df = df.dropna(subset="motionIndex")

    new_cols = fields[:5]
    new_cols += ["submissionTime", "approvalThreshold", "ayeVoters",
                 "nayVoters", "lastUpdateTime", "subsquareCommentCount",
                 "polkassemblyCommentCount", "treasuryBounties",
                 "treasuryProposals", "externalProposals"]
    df = df.set_axis(new_cols, axis=1)

    return df


if __name__ == "__main__":
    motions = get_data("kusama")
    df = pr.trim_long_strings(motions, ["title", "content"])
    df = pr.trim_account_id_strings(df, ["proposer"])
    pr.print_long_df(df)
