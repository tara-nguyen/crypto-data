import governanceReport.print_helpers as pr
from governanceReport.sources.opensquare import Extractor, Transformer
from string import Template
from concurrent.futures import ThreadPoolExecutor


def get_referenda(network):
    fields = ["referendumIndex", "track", "state_name", "proposer", "title",
              "content", "createdAt", "onchainData_info_deciding_since",
              "onchainData_lastConfirmStartedAt_blockHeight",
              "onchainData_lastConfirmStartedAt_blockTime",
              "onchainData_info_deciding_confirming", "onchainData_tally_ayes",
              "onchainData_tally_nays", "onchainData_tally_support",
              "onchainData_tally_electorate", "onchainData_approved",
              "onchainData_info_enactment_after",
              "onchainData_info_enactment_at", "onchainData_enactment_when",
              "state_args_result_ok", "state_indexer_blockTime",
              "commentsCount", "polkassemblyCommentsCount",
              "onchainData_fellowshipReferenda",
              "onchainData_treasuryProposalIndexes",
              "onchainData_treasuryBounties"]
    token_cols = ["onchainData_tally_ayes", "onchainData_tally_nays",
                  "onchainData_tally_support", "onchainData_tally_electorate"]
    time_cols = ["onchainData_lastConfirmStartedAt_blockTime",
                  "state_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", network, "/gov2/referendums").extract()
    df = Transformer(data).transform(fields, token_cols, network, time_cols)

    df["onchainData_approved"] = df["onchainData_approved"].map(
        lambda x: x[0], na_action="ignore")

    new_cols = ["id", "track", "state"] + fields[3:6]
    new_cols += ["submissionTime", "decisionStartBlock",
                 "confirmationStartBlock", "confirmationStartTime",
                 "confirmedAtBlock", "aye", "nay", "support", "electorate",
                 "approvedAtBlock", "enactmentDelayProposed",
                 "enactedAtBlockProposed", "enactedAtBlockActual",
                 "executionResult", "lastUpdateTime", "subsquareCommentCount",
                 "polkassemblyCommentCount", "fellowshipRefs",
                 "treasuryProposals", "treasuryBounties"]
    df = df.set_axis(new_cols, axis=1)

    return df


def get_referendum_votes(network):
    df_referenda = get_referenda(network)
    ref_ids = df_referenda["id"]

    fields = ["referendumIndex", "voter", "indexer_blockTime", "isStandard",
              "isSplit", "isSplitAbstain", "vote_balance", "vote_aye",
              "vote_nay", "vote_abstain", "vote_vote_isAye",
              "vote_vote_conviction", "indexer_blockHeight",
              "indexer_extrinsicIndex"]
    token_cols = ["vote_balance", "vote_aye", "vote_nay", "vote_abstain"]
    time_cols = ["indexer_blockTime"]
    route_template = Template("/gov2/referendums/$ref_id/vote-calls")
    routes = [route_template.substitute(ref_id=rid) for rid in ref_ids]

    with ThreadPoolExecutor() as exe:
        futures = [exe.submit(Extractor("subsquare", network, route).extract)
                   for route in routes]
    data = sum([future.result() for future in futures], [])
    df_votes = Transformer(data).transform(
        fields, token_cols, network, time_cols,
        sort_by=["indexer_blockTime", "referendumIndex"], ascending=False)

    new_cols = fields[:2] + ["voteTime"] + fields[3:6]
    new_cols += ["standardVote", "splitAyeVote", "splitNayVote", "abstainVote",
                 "standardIsAye", "standardConviction", "blockId",
                 "extrinsicId"]
    df_votes = df_votes.set_axis(new_cols, axis=1)

    df = {"referenda": df_referenda, "votes": df_votes}

    return df


def get_data(network):
    df = get_referendum_votes(network)

    return df


if __name__ == "__main__":
    ref = get_data("kusama")
    df = pr.trim_long_strings(ref["referenda"], ["title", "content"])
    df = pr.trim_account_id_strings(df, ["proposer"])
    pr.print_long_df(df)
    print()
    df = pr.trim_account_id_strings(ref["votes"], ["voter"])
    pr.print_long_df(df)
