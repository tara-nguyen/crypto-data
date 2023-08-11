import pandas as pd
from quarterlyReport.governance.sources.opensquare import OpensquareExtractor
from quarterlyReport.governance.sources.opensquare import OpensquareTransformer
from quarterlyReport.governance.subsquare import tracks
from quarterlyReport.governance.dotreasury import proposals, bounties


def get_data(network):
    df_tracks = tracks.get_data(network)

    fields = ["referendumIndex", "track", "state_name",
              "onchainData_tally_support", "onchainData_tally_electorate",
              "onchainData_treasuryProposalIndexes",
              "onchainData_treasuryBounties"]
    token_cols = ["onchainData_tally_support", "onchainData_tally_electorate"]
    data = OpensquareExtractor("subsquare", network,
                               "/gov2/referendums").extract()

    trf = OpensquareTransformer(data)
    df = trf.to_frame(fields, token_cols, network)

    df = df.set_axis(["id", "track", "state", "support", "electorate",
                      "treasuryProposals", "treasuryBounties"], axis=1)
    df = df.eval("supportRate = support / electorate")
    df = df.replace(["Executed", "Confirmed"], "Approved").replace(
        ["Preparing", "Deciding", "Confirming"], "Pending").replace(
        ["Rejected", "Cancelled", "TimedOut"], "Not Approved")

    df_counts = df.groupby(["track", "state"])["id"].count()
    df_counts = df_counts.unstack(fill_value=0).reset_index()
    df_counts = df_counts.merge(df_tracks, left_on="track", right_on="id")
    df_counts["track"] = df_counts["name"]
    df_counts = df_counts.drop(columns=df_tracks.columns)

    df_support = df.groupby("track")["supportRate"].mean().reset_index()
    df_support = df_support.merge(df_tracks, left_on="track", right_on="id")
    df_support["track"] = df_support["name"]
    df_support = df_support.drop(columns=df_tracks.columns)

    df_approved = df.query("state == 'Approved'")
    df_proposals = df_approved.reindex(columns=["id", "supportRate",
                                                "treasuryProposals"])
    df_proposals = df_proposals.explode("treasuryProposals").dropna()
    df_proposals = df_proposals.merge(
        proposals.get_data(network), left_on="treasuryProposals",
        right_on="proposalIndex")
    df_proposals = df_proposals.reindex(columns=["id", "value", "supportRate"])

    df_bounties = df_approved.reindex(columns=["id", "supportRate",
                                               "treasuryBounties"])
    df_bounties = df_bounties.explode("treasuryBounties").dropna()
    df_bounties["bountyIndex"] = df_bounties["treasuryBounties"].map(
        lambda x: x["index"] if x["method"] == "approveBounty" else None)
    df_bounties = df_bounties.merge(bounties.get_data(network))
    df_bounties = df_bounties.reindex(columns=["id", "value", "supportRate"])

    df_spend = pd.concat([df_proposals, df_bounties])
    df_spend = df_spend.sort_values("value", ascending=False)

    dfs = {"counts": df_counts, "support": df_support, "spend": df_spend}

    return dfs


if __name__ == "__main__":
    refs = get_data("polkadot")
    # for df in refs.values():
    #     print()
    #     print(df.to_string())
