import governanceReport.print_helpers as pr
from reports.governance_etl import GovernanceReport
from governanceReport.sources.opensquare import Extractor, Transformer


def get_data(network):
    fields = ["proposalIndex", "state", "proposer", "title", "content",
              "createdAt", "onchainData_deposit",
              "proposalState_indexer_blockTime", "commentsCount",
              "polkassemblyCommentsCount", "referendumIndex"]
    time_cols = ["proposalState_indexer_blockTime", "createdAt"]

    data = Extractor("subsquare", network, "/democracy/proposals").extract()
    df = Transformer(data).transform(fields, time_cols=time_cols)

    new_col = "depositor"
    target_col = "onchainData_deposit"
    df[target_col] = df[target_col].map(
        lambda x: x[::-1] if isinstance(x[0], int) else x)
    df[new_col] = df[target_col].map(lambda x: x[0][0])
    df[target_col] = df[target_col].map(
        lambda x: x[1] / GovernanceReport().networks[network])

    cols = fields.copy()
    cols.insert(fields.index(target_col), new_col)
    df = df.reindex(columns=cols)
    df = df.rename(columns={target_col: "submissionDeposit"})

    return df


if __name__ == "__main__":
    proposals = get_data("kusama")
    df = pr.trim_long_strings(proposals, ["title", "content"])
    df = pr.trim_account_id_strings(df, ["proposer", "depositor"])
    pr.print_long_df(df)
